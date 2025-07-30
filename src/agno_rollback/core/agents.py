"""Agent configurations for the workflow resumption system.

This module defines the AI agents used in workflows:
- Web search agent
- News search agent  
- Summarization agent
"""

from typing import List, Optional, Any
import os
from agno.agent import Agent, Toolkit


class AgentFactory:
    """Factory for creating configured agents."""

    @staticmethod
    def _default_openai_chat(model_id: str = "gpt-4o-mini"):
        """Return a standard OpenAIChat model configured via environment variables.
        
        Args:
            model_id: The model identifier (e.g., "gpt-4o-mini", "gpt-4o")
        """
        from agno.models.openai import OpenAIChat  # Local import to avoid heavy import cost when not needed
        return OpenAIChat(
            id=model_id,
            base_url=os.getenv("BASE_URL"),
            api_key=os.getenv("OPENAI_API_KEY"),
        )

    @staticmethod
    def create_web_search_agent(
        model: Optional[Any] = None,
        tools: Optional[List[Toolkit]] = None,
        name: str = "WebSearchAgent",
        search_provider: str = "google"  # Options: google, baidu, duckduckgo, brave, tavily
    ) -> Agent:
        """Create a web search agent.
        
        Args:
            model: Language model to use (defaults to gpt-4)
            tools: Additional tools for the agent
            name: Agent name
            
        Returns:
            Configured web search agent
        """
        from agno.models.openai import OpenAIChat
        from agno.tools.duckduckgo import DuckDuckGoTools
        
        # Default model if not provided
        if model is None:
            model = AgentFactory._default_openai_chat("gpt-4o-mini")
        
        # Default tools based on search provider
        if search_provider == "google":
            from agno.tools.googlesearch import GoogleSearchTools
            default_tools = [GoogleSearchTools()]
        elif search_provider == "baidu":
            from agno.tools.baidusearch import BaiduSearchTools
            default_tools = [BaiduSearchTools()]
        elif search_provider == "brave":
            from agno.tools.brave import BraveSearchTools
            default_tools = [BraveSearchTools()]
        elif search_provider == "tavily":
            from agno.tools.tavily import TavilyTools
            default_tools = [TavilyTools()]
        else:  # Default to duckduckgo
            default_tools = [DuckDuckGoTools()]
            
        if tools:
            default_tools.extend(tools)
        
        return Agent(
            name=name,
            model=model,
            tools=default_tools,
            instructions="""You are a web search specialist. Your task is to:
1. Search for relevant information based on the user's query
2. Focus on finding recent, accurate, and comprehensive information
3. Prioritize authoritative sources
4. Extract key facts and insights
5. Organize findings in a clear, structured format

When searching:
- Use multiple search queries if needed to cover different aspects
- Look for recent updates and current information
- Verify information across multiple sources when possible
- Note the source and date of information found

Return results in a structured format with:
- Key findings
- Source URLs
- Relevance to the query
- Date of information (if available)
""",
            markdown=True,
            show_tool_calls=True,
        )
    
    @staticmethod
    def create_news_search_agent(
        model: Optional[Any] = None,
        tools: Optional[List[Toolkit]] = None,
        name: str = "NewsSearchAgent"
    ) -> Agent:
        """Create a news search agent.
        
        Args:
            model: Language model to use
            tools: Additional tools for the agent
            name: Agent name
            
        Returns:
            Configured news search agent
        """
        from agno.models.openai import OpenAIChat
        from agno.tools.hackernews import HackerNewsTools
        
        # Default model if not provided
        if model is None:
            model = AgentFactory._default_openai_chat("gpt-4o-mini")
        
        # Default tools
        default_tools = [HackerNewsTools()]
        if tools:
            default_tools.extend(tools)
        
        return Agent(
            name=name,
            model=model,
            tools=default_tools,
            instructions="""You are a news search specialist focusing on technology and current events. Your task is to:
1. Find the latest news related to the user's query
2. Focus on recent developments and breaking news
3. Identify trending topics and discussions
4. Extract key insights from news articles
5. Provide context and analysis

When searching news:
- Prioritize recent articles (within the last week/month)
- Look for multiple perspectives on the topic
- Identify key trends and patterns
- Note any controversial or disputed information

Return results in a structured format with:
- Headline and summary
- Source and publication date
- Key insights
- Relevance to the query
- Any notable discussions or reactions
""",
            markdown=True,
            show_tool_calls=True,
        )
    
    @staticmethod
    def create_summarization_agent(
        model: Optional[Any] = None,
        name: str = "SummarizationAgent"
    ) -> Agent:
        """Create a summarization agent.
        
        Args:
            model: Language model to use (defaults to gpt-4)
            name: Agent name
            
        Returns:
            Configured summarization agent
        """
        from agno.models.openai import OpenAIChat
        
        # Default model if not provided - use a more capable model for summarization
        if model is None:
            model = AgentFactory._default_openai_chat("gpt-4o")
        
        return Agent(
            name=name,
            model=model,
            instructions="""You are an expert summarization specialist. Your task is to:
1. Synthesize information from multiple sources (web and news searches)
2. Create a comprehensive yet concise summary
3. Highlight key findings and insights
4. Identify patterns and connections
5. Provide actionable conclusions

When summarizing:
- Start with an executive summary (2-3 sentences)
- Organize information by themes or importance
- Highlight any contradictions or uncertainties
- Include relevant dates and sources
- End with key takeaways or recommendations

Structure your summary as:
1. **Executive Summary**: Brief overview
2. **Key Findings**: Main discoveries from searches
3. **Detailed Analysis**: Deeper insights organized by theme
4. **Trends & Patterns**: Notable patterns identified
5. **Conclusions**: Final thoughts and recommendations

Keep the tone professional and objective. Focus on providing value to the user.
""",
            markdown=True,
            response_model=None,  # Allow free-form responses
        )
    
    @staticmethod
    def create_coordinator_agent(
        model: Optional[Any] = None,
        name: str = "CoordinatorAgent"
    ) -> Agent:
        """Create a workflow coordinator agent.
        
        This agent helps coordinate complex workflows and make decisions.
        
        Args:
            model: Language model to use
            name: Agent name
            
        Returns:
            Configured coordinator agent
        """
        from agno.models.openai import OpenAIChat
        
        # Default model if not provided
        if model is None:
            model = AgentFactory._default_openai_chat("gpt-4o-mini")
        
        return Agent(
            name=name,
            model=model,
            instructions="""You are a workflow coordinator responsible for:
1. Analyzing user queries to determine the best approach
2. Deciding which agents to invoke and in what order
3. Monitoring workflow progress
4. Handling edge cases and errors
5. Ensuring comprehensive results

Your responsibilities:
- Parse and understand user intent
- Break down complex queries into sub-tasks
- Coordinate between different agents
- Ensure all aspects of the query are addressed
- Handle any conflicts or inconsistencies in results

Always aim for:
- Completeness: Address all aspects of the user's query
- Accuracy: Ensure information is reliable
- Efficiency: Avoid redundant operations
- Clarity: Present results in an organized manner
""",
            markdown=True,
        )


class AgentConfig:
    """Configuration class for agent settings."""
    
    # Model configurations
    DEFAULT_SEARCH_MODEL = "gpt-4o-mini"
    DEFAULT_SUMMARY_MODEL = "gpt-4o"
    
    # Timeout settings (in seconds)
    SEARCH_TIMEOUT = 30
    SUMMARY_TIMEOUT = 60
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0
    
    # Search settings
    MAX_SEARCH_RESULTS = 10
    MAX_NEWS_ITEMS = 20
    
    @classmethod
    def get_default_config(cls) -> dict:
        """Get default configuration dictionary."""
        return {
            "models": {
                "search": cls.DEFAULT_SEARCH_MODEL,
                "summary": cls.DEFAULT_SUMMARY_MODEL,
            },
            "timeouts": {
                "search": cls.SEARCH_TIMEOUT,
                "summary": cls.SUMMARY_TIMEOUT,
            },
            "retries": {
                "max_attempts": cls.MAX_RETRIES,
                "delay": cls.RETRY_DELAY,
            },
            "search": {
                "max_results": cls.MAX_SEARCH_RESULTS,
                "max_news": cls.MAX_NEWS_ITEMS,
            }
        }