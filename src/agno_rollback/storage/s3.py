"""S3 storage backend implementation.

This implementation handles blob storage (large attachments, raw data, binary content).
Uses aioboto3 for async S3 operations.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from uuid import UUID
import hashlib

from ..core.models import SessionContext, WorkflowState, WorkflowTask
from .base import StorageBackend


class S3Storage(StorageBackend):
    """S3 storage backend for blob operations.
    
    Note: This backend primarily handles blob storage.
    For transactional operations, it delegates to a primary storage backend.
    """
    
    def __init__(
        self, 
        bucket_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        primary_backend: Optional[StorageBackend] = None
    ):
        """Initialize S3 storage.
        
        Args:
            bucket_name: S3 bucket name (defaults to env variable)
            aws_access_key_id: AWS access key ID (defaults to env variable)
            aws_secret_access_key: AWS secret access key (defaults to env variable)
            region_name: AWS region name (defaults to env variable)
            endpoint_url: Custom endpoint URL for S3-compatible services (defaults to env variable)
            primary_backend: Primary storage backend for transactional operations
        """
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "agno-rollback-blobs")
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL")  # For MinIO, LocalStack, etc.
        
        self.session = None
        self.primary_backend = primary_backend
    
    async def initialize(self) -> None:
        """Initialize S3 connection and create bucket."""
        try:
            import aioboto3
            self.session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            await self._ensure_bucket_exists()
        except ImportError:
            raise ImportError(
                "aioboto3 is required for S3 backend. "
                "Install it with: pip install aioboto3"
            )
    
    async def close(self) -> None:
        """Close S3 session."""
        # aioboto3 sessions don't need explicit closing
        self.session = None
    
    async def _ensure_bucket_exists(self) -> None:
        """Ensure the S3 bucket exists."""
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket_name)
            except Exception:
                # Bucket doesn't exist, create it
                try:
                    if self.region_name == 'us-east-1':
                        await s3.create_bucket(Bucket=self.bucket_name)
                    else:
                        await s3.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region_name}
                        )
                    
                    # Set up lifecycle configuration for automatic cleanup
                    await s3.put_bucket_lifecycle_configuration(
                        Bucket=self.bucket_name,
                        LifecycleConfiguration={
                            'Rules': [
                                {
                                    'ID': 'DeleteOldBlobs',
                                    'Status': 'Enabled',
                                    'Filter': {'Prefix': 'blobs/'},
                                    'Expiration': {'Days': 90}
                                },
                                {
                                    'ID': 'TransitionToIA',
                                    'Status': 'Enabled',
                                    'Filter': {'Prefix': 'blobs/'},
                                    'Transitions': [
                                        {
                                            'Days': 30,
                                            'StorageClass': 'STANDARD_IA'
                                        },
                                        {
                                            'Days': 60,
                                            'StorageClass': 'GLACIER'
                                        }
                                    ]
                                }
                            ]
                        }
                    )
                except Exception as e:
                    print(f"Warning: Could not create bucket {self.bucket_name}: {e}")
    
    # Blob storage methods (S3 strengths)
    
    async def put_blob(
        self,
        key: str,
        data: Union[bytes, str],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Store blob data in S3.
        
        Args:
            key: S3 object key
            data: Data to store
            content_type: MIME type of the data
            metadata: Additional metadata to store with the object
            
        Returns:
            Dictionary with blob information
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        # Calculate hash for integrity
        sha256_hash = hashlib.sha256(data).hexdigest()
        
        # Prepare metadata
        s3_metadata = metadata or {}
        s3_metadata.update({
            'sha256': sha256_hash,
            'upload_timestamp': datetime.utcnow().isoformat(),
            'size': str(len(data))
        })
        
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            await s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type,
                Metadata=s3_metadata
            )
        
        return {
            "key": key,
            "size": len(data),
            "sha256": sha256_hash,
            "content_type": content_type,
            "metadata": s3_metadata,
            "url": f"s3://{self.bucket_name}/{key}"
        }
    
    async def get_blob(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve blob data from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            Dictionary with blob data and metadata, or None if not found
        """
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            try:
                response = await s3.get_object(Bucket=self.bucket_name, Key=key)
                data = await response['Body'].read()
                
                return {
                    "key": key,
                    "data": data,
                    "size": response['ContentLength'],
                    "content_type": response['ContentType'],
                    "metadata": response.get('Metadata', {}),
                    "last_modified": response['LastModified'],
                    "etag": response['ETag'].strip('"')
                }
            except Exception:
                return None
    
    async def delete_blob(self, key: str) -> bool:
        """Delete blob from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            True if deleted successfully, False otherwise
        """
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            try:
                await s3.delete_object(Bucket=self.bucket_name, Key=key)
                return True
            except Exception:
                return False
    
    async def list_blobs(
        self, 
        prefix: str = "", 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List blobs with optional prefix filter.
        
        Args:
            prefix: Key prefix to filter by
            limit: Maximum number of objects to return
            
        Returns:
            List of blob metadata
        """
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            try:
                response = await s3.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=limit
                )
                
                return [
                    {
                        "key": obj['Key'],
                        "size": obj['Size'],
                        "last_modified": obj['LastModified'],
                        "etag": obj['ETag'].strip('"')
                    }
                    for obj in response.get('Contents', [])
                ]
            except Exception:
                return []
    
    async def get_presigned_url(
        self, 
        key: str, 
        expiration: int = 3600,
        method: str = "get_object"
    ) -> Optional[str]:
        """Generate a presigned URL for blob access.
        
        Args:
            key: S3 object key
            expiration: URL expiration time in seconds
            method: HTTP method ('get_object' or 'put_object')
            
        Returns:
            Presigned URL or None if failed
        """
        async with self.session.client(
            's3', 
            endpoint_url=self.endpoint_url
        ) as s3:
            try:
                url = await s3.generate_presigned_url(
                    method,
                    Params={'Bucket': self.bucket_name, 'Key': key},
                    ExpiresIn=expiration
                )
                return url
            except Exception:
                return None
    
    # Convenience methods for workflow data
    
    async def store_agent_output_blob(
        self,
        task_id: UUID,
        agent_name: str,
        output_content: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store large agent output as blob.
        
        Args:
            task_id: Task ID
            agent_name: Name of the agent
            output_content: Large output content
            metadata: Additional metadata
            
        Returns:
            Blob storage information
        """
        key = f"agent_outputs/{task_id}/{agent_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
        
        blob_metadata = {
            'task_id': str(task_id),
            'agent_name': agent_name,
            'content_type': 'agent_output'
        }
        if metadata:
            blob_metadata.update({k: str(v) for k, v in metadata.items()})
        
        return await self.put_blob(
            key=key,
            data=output_content,
            content_type="text/plain",
            metadata=blob_metadata
        )
    
    async def store_tool_result_blob(
        self,
        task_id: UUID,
        tool_name: str,
        result_data: Union[str, bytes],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store large tool result as blob.
        
        Args:
            task_id: Task ID
            tool_name: Name of the tool
            result_data: Large result data
            content_type: MIME type of the data
            metadata: Additional metadata
            
        Returns:
            Blob storage information
        """
        key = f"tool_results/{task_id}/{tool_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        blob_metadata = {
            'task_id': str(task_id),
            'tool_name': tool_name,
            'content_type': 'tool_result'
        }
        if metadata:
            blob_metadata.update({k: str(v) for k, v in metadata.items()})
        
        return await self.put_blob(
            key=key,
            data=result_data,
            content_type=content_type,
            metadata=blob_metadata
        )
    
    async def store_checkpoint_blob(
        self,
        task_id: UUID,
        checkpoint_name: str,
        checkpoint_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Store large checkpoint data as blob.
        
        Args:
            task_id: Task ID
            checkpoint_name: Name of the checkpoint
            checkpoint_data: Checkpoint data
            
        Returns:
            Blob storage information
        """
        key = f"checkpoints/{task_id}/{checkpoint_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        blob_metadata = {
            'task_id': str(task_id),
            'checkpoint_name': checkpoint_name,
            'content_type': 'checkpoint'
        }
        
        return await self.put_blob(
            key=key,
            data=json.dumps(checkpoint_data, indent=2),
            content_type="application/json",
            metadata=blob_metadata
        )
    
    async def get_task_blobs(self, task_id: UUID) -> List[Dict[str, Any]]:
        """Get all blobs associated with a task.
        
        Args:
            task_id: Task ID
            
        Returns:
            List of blob metadata for the task
        """
        prefixes = [
            f"agent_outputs/{task_id}/",
            f"tool_results/{task_id}/",
            f"checkpoints/{task_id}/"
        ]
        
        all_blobs = []
        for prefix in prefixes:
            blobs = await self.list_blobs(prefix=prefix)
            all_blobs.extend(blobs)
        
        return all_blobs
    
    async def cleanup_task_blobs(self, task_id: UUID) -> int:
        """Clean up all blobs associated with a task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Number of blobs deleted
        """
        blobs = await self.get_task_blobs(task_id)
        deleted_count = 0
        
        for blob in blobs:
            if await self.delete_blob(blob['key']):
                deleted_count += 1
        
        return deleted_count
    
    # Delegate transactional operations to primary backend
    
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.create_task(task)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_task(task_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.update_task(task)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def list_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WorkflowTask]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.list_tasks(user_id, session_id, status, limit, offset)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def delete_task(self, task_id: UUID) -> bool:
        """Delegate to primary backend and clean up blobs."""
        if self.primary_backend:
            result = await self.primary_backend.delete_task(task_id)
            if result:
                # Clean up associated blobs
                await self.cleanup_task_blobs(task_id)
            return result
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.save_workflow_state(state)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_workflow_state(task_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.update_workflow_state(state)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_session(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.save_session(session)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.delete_session(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_resumable_tasks(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Enhanced execution history with blob storage information."""
        base_history = {}
        if self.primary_backend:
            base_history = await self.primary_backend.get_task_execution_history(task_id)
        
        # Add blob storage information
        blobs = await self.get_task_blobs(task_id)
        
        blob_summary = {
            "total_blobs": len(blobs),
            "total_size": sum(blob.get('size', 0) for blob in blobs),
            "blob_types": {},
            "blobs": blobs
        }
        
        # Categorize blobs by type
        for blob in blobs:
            if 'agent_outputs' in blob['key']:
                blob_summary["blob_types"].setdefault('agent_outputs', 0)
                blob_summary["blob_types"]['agent_outputs'] += 1
            elif 'tool_results' in blob['key']:
                blob_summary["blob_types"].setdefault('tool_results', 0)
                blob_summary["blob_types"]['tool_results'] += 1
            elif 'checkpoints' in blob['key']:
                blob_summary["blob_types"].setdefault('checkpoints', 0)
                blob_summary["blob_types"]['checkpoints'] += 1
        
        base_history.update({
            "s3_blob_storage": blob_summary
        })
        
        return base_history
    
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Delegate to primary backend and clean up old blobs."""
        deleted_count = 0
        if self.primary_backend:
            deleted_count = await self.primary_backend.cleanup_old_tasks(days)
        
        # S3 lifecycle policies handle automatic cleanup, but we can also manually clean
        # old blobs that match certain patterns
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # List and delete old blobs (this is a simplified approach)
        # In production, you'd want more sophisticated cleanup logic
        all_blobs = await self.list_blobs(limit=1000)
        old_blob_count = 0
        
        for blob in all_blobs:
            if blob['last_modified'] < cutoff_date:
                if await self.delete_blob(blob['key']):
                    old_blob_count += 1
        
        return deleted_count 