"""Retrieval and summarization workflow implementation.

This workflow:
1. Performs parallel web and news searches
2. Summarizes the combined results
3. Supports resumption from any point
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional

import logging
logger = logging.getLogger(__name__)

from ..core import (
    AgentFactory,
    ParallelRetrievalMixin,
    ResumableWorkflow,
    ResumptionPoint,
)
from ..monitoring import EventType, MonitoringContext


class RetrievalSummarizeWorkflow(ResumableWorkflow, ParallelRetrievalMixin):
    """Workflow that retrieves information and summarizes it.
    
    This workflow:
    1. Searches web for information
    2. Searches news for recent updates
    3. Combines and summarizes all findings
    """
    
    def __init__(self, monitor=None, **kwargs):
        """Initialize the workflow.
        
        Args:
            monitor: Optional workflow monitor
            **kwargs: Additional arguments for ResumableWorkflow
        """
        super().__init__(
            task_id=kwargs['task_id'],
            user_id=kwargs['user_id'],
            session_id=kwargs['session_id'],
            query=kwargs['query'],
            storage=kwargs['storage'],
            resume_from=kwargs.get('resume_from')
        )
        self.monitor = monitor
        
        # Lazy agent initialization - agents will be created on first use
        self._web_agent = None
        self._news_agent = None
        self._summary_agent = None
        self._agents_initialized = False
    
    @property
    def web_agent(self):
        """Lazy initialization of web agent."""
        if self._web_agent is None:
            import os
            search_provider = os.getenv("SEARCH_PROVIDER", "google")
            self._web_agent = AgentFactory.create_web_search_agent(search_provider=search_provider)
        return self._web_agent
    
    @property
    def news_agent(self):
        """Lazy initialization of news agent."""
        if self._news_agent is None:
            self._news_agent = AgentFactory.create_news_search_agent()
        return self._news_agent
    
    @property
    def summary_agent(self):
        """Lazy initialization of summary agent."""
        if self._summary_agent is None:
            self._summary_agent = AgentFactory.create_summarization_agent()
        return self._summary_agent
    
    async def execute(self) -> str:
        """Execute the workflow with resumption support.
        
        Returns:
            Final summarized result
        """
        try:
            # Step 1: Initialize (if not resuming)
            if not self.resume_from or self.should_resume_from(ResumptionPoint.RESTART_FROM_BEGINNING):
                await self._initialize_workflow()
            
            # Step 2: Parallel Retrieval
            if not self.resume_from or self.should_resume_from(ResumptionPoint.RESUME_BEFORE_PARALLEL_RETRIEVAL):
                await self._perform_parallel_retrieval()
            
            # Step 3: Summarization
            if not self.resume_from or self.should_resume_from(ResumptionPoint.RESUME_BEFORE_SUMMARIZATION):
                result = await self._perform_summarization()
                return result
            
            # If we get here, return cached result
            if self.task and self.task.result:
                return self.task.result
            else:
                raise ValueError("No result available and no steps to execute")
                
        except Exception as e:
            logger.error(f"Workflow execution error: {e}", exc_info=True)
            raise
    
    async def _initialize_workflow(self) -> None:
        """Initialize the workflow."""
        logger.info(f"Initializing workflow for query: {self.query}")
        
        await self.update_progress("initialization", 10.0)
        
        # Save initial checkpoint
        await self.save_checkpoint("initialized", {
            "query": self.query,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        if self.monitor:
            await self.monitor.track_workflow_start(
                self.task_id,
                self.user_id,
                self.query
            )
    
    async def _perform_parallel_retrieval(self) -> None:
        """Perform parallel web and news retrieval."""
        logger.info("Starting parallel retrieval")
        
        await self.update_progress("parallel_retrieval", 20.0)
        
        # Check what needs to be retrieved based on resumption
        skip_web = False
        skip_news = False
        
        if self.resume_from:
            if self.workflow_state:
                skip_web = self.workflow_state.web_retrieval_completed
                skip_news = self.workflow_state.news_retrieval_completed
            
            # Load cached results if available
            if skip_web and self.task.web_retrieval_results:
                self.session_state["web_results"] = self.task.web_retrieval_results
            if skip_news and self.task.news_retrieval_results:
                self.session_state["news_results"] = self.task.news_retrieval_results
        
        # Perform retrieval
        results = await self.parallel_retrieval(
            self._search_web,
            self._search_news,
            skip_web=skip_web,
            skip_news=skip_news
        )
        
        # Update task with results
        if results["web_results"] and not skip_web:
            self.task.web_retrieval_results = results["web_results"]
            self.session_state["web_results"] = results["web_results"]
            if self.workflow_state:
                # Only mark as completed if the result was successful
                self.workflow_state.web_retrieval_completed = results["web_results"].get("success", False)
        
        if results["news_results"] and not skip_news:
            self.task.news_retrieval_results = results["news_results"]
            self.session_state["news_results"] = results["news_results"]
            if self.workflow_state:
                # Only mark as completed if the result was successful
                self.workflow_state.news_retrieval_completed = results["news_results"].get("success", False)
        
        # Save checkpoint
        await self.save_checkpoint("parallel_retrieval_completed", {
            "web_completed": self.workflow_state.web_retrieval_completed,
            "news_completed": self.workflow_state.news_retrieval_completed
        })
        
        await self.update_progress("parallel_retrieval", 60.0)
        await self.storage.update_task(self.task)
        await self.storage.update_workflow_state(self.workflow_state)
    
    async def _search_web(self) -> Dict[str, Any]:
        """Perform web search."""
        logger.info(f"Searching web for: {self.query}")
        
        if self.monitor:
            async with MonitoringContext(
                self.monitor,
                self.task_id,
                "web_search",
                EventType.WEB_RETRIEVAL_STARTED
            ):
                # Add event
                self.task.add_event("web_retrieval_started", {
                    "query": self.query,
                    "timestamp": datetime.utcnow().isoformat()
                })
                
                # Run web search agent
                try:
                    response = self.web_agent.run(self.query)
                    
                    # Process response
                    result = {
                        "success": True,
                        "query": self.query,
                        "results": response.content if hasattr(response, 'content') else str(response),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    # Record agent output
                    await self.add_agent_output(
                        "web_search_agent",
                        result["results"],
                        {"query": self.query}
                    )
                    
                    await self.update_progress("web_retrieval", 40.0)
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"Web search failed: {e}")
                    return {
                        "success": False,
                        "error": str(e),
                        "query": self.query
                    }
    
    async def _search_news(self) -> Dict[str, Any]:
        """Perform news search."""
        logger.info(f"Searching news for: {self.query}")
        
        if self.monitor:
            async with MonitoringContext(
                self.monitor,
                self.task_id,
                "news_search",
                EventType.NEWS_RETRIEVAL_STARTED
            ):
                # Add event
                self.task.add_event("news_retrieval_started", {
                    "query": self.query,
                    "timestamp": datetime.utcnow().isoformat()
                })
                
                # Run news search agent
                try:
                    response = self.news_agent.run(self.query)
                    
                    # Process response
                    result = {
                        "success": True,
                        "query": self.query,
                        "results": response.content if hasattr(response, 'content') else str(response),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    # Record agent output
                    await self.add_agent_output(
                        "news_search_agent",
                        result["results"],
                        {"query": self.query}
                    )
                    
                    await self.update_progress("news_retrieval", 50.0)
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"News search failed: {e}")
                    return {
                        "success": False,
                        "error": str(e),
                        "query": self.query
                    }
    
    async def _perform_summarization(self) -> str:
        """Perform summarization of all results."""
        logger.info("Starting summarization")
        
        await self.update_progress("summarization", 70.0)
        
        # Mark summarization as started
        if self.workflow_state:
            self.workflow_state.summarization_started = True
            await self.storage.update_workflow_state(self.workflow_state)
        
        self.task.add_event("summarization_started", {
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Prepare context for summarization
        web_results = self.session_state.get("web_results", {})
        news_results = self.session_state.get("news_results", {})
        
        # Build summarization prompt
        summary_context = self._build_summary_context(web_results, news_results)
        
        if self.monitor:
            async with MonitoringContext(
                self.monitor,
                self.task_id,
                "summarization",
                EventType.SUMMARIZATION_STARTED
            ):
                try:
                    # Run summary agent
                    response = self.summary_agent.run(summary_context)
                    
                    summary = response.content if hasattr(response, 'content') else str(response)
                    
                    # Record agent output
                    await self.add_agent_output(
                        "summarization_agent",
                        summary,
                        {
                            "has_web_results": bool(web_results),
                            "has_news_results": bool(news_results)
                        }
                    )
                    
                    # Mark as completed
                    if self.workflow_state:
                        self.workflow_state.summarization_completed = True
                        await self.storage.update_workflow_state(self.workflow_state)
                    
                    await self.update_progress("summarization", 100.0)
                    
                    return summary
                    
                except Exception as e:
                    logger.error(f"Summarization failed: {e}")
                    raise
    
    def _build_summary_context(
        self,
        web_results: Dict[str, Any],
        news_results: Dict[str, Any]
    ) -> str:
        """Build context for summarization.
        
        Args:
            web_results: Web search results
            news_results: News search results
            
        Returns:
            Formatted context for summarization
        """
        context_parts = [
            f"Query: {self.query}",
            "\n## Information to Summarize:\n"
        ]
        
        # Add web results
        if web_results and web_results.get("success"):
            context_parts.append("### Web Search Results:")
            context_parts.append(web_results.get("results", "No results"))
            context_parts.append("")
        
        # Add news results
        if news_results and news_results.get("success"):
            context_parts.append("### News Search Results:")
            context_parts.append(news_results.get("results", "No results"))
            context_parts.append("")
        
        # Add instructions
        context_parts.append("\n## Instructions:")
        context_parts.append("Please provide a comprehensive summary of the above information.")
        context_parts.append("Focus on key findings, recent developments, and actionable insights.")
        
        return "\n".join(context_parts)