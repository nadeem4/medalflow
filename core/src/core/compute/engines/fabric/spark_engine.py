"""Microsoft Fabric Spark engine implementation.

This module provides a comprehensive Spark engine implementation for Microsoft Fabric.
The engine handles Spark job submission, monitoring, and management using the Fabric
REST API, OneLake storage integration, and workspace compute resources.

Features:
    - **Batch Jobs**: Traditional fire-and-forget Spark job execution
    - **Interactive Sessions**: Real-time code execution with state preservation
    - **Notebook Execution**: Jupyter notebook processing with parameters
    - **Job Definitions**: Reusable parameterized job templates
    - **File Management**: Upload files to OneLake for job execution
    - **OneLake Integration**: Native integration with Fabric's storage layer
    - **Comprehensive Monitoring**: Real-time status, logs, and metrics
    - **Error Handling**: Robust retry logic and error recovery

Fabric Architecture:
    Microsoft Fabric provides a unified analytics platform with:
    1. **OneLake**: Centralized data lake storage
    2. **Lakehouse**: Delta Lake-based analytics storage
    3. **Spark Compute**: Managed Apache Spark clusters
    4. **Workspace**: Logical container for all resources

Job Submission Process:
    1. **File Upload**: Upload job files to OneLake (required for Fabric)
    2. **Job Definition**: Create job definition with main file and dependencies
    3. **Parameter Injection**: Inject runtime parameters
    4. **Execution**: Submit to Fabric Spark compute
    5. **Monitoring**: Track progress and collect results

Authentication:
    Uses Azure Active Directory (Entra ID) for authentication:
    - Service principal authentication
    - Managed identity support
    - Workspace-level permissions
    - OneLake access control

OneLake Integration:
    - Files uploaded to workspace-specific OneLake paths
    - Automatic lakehouse mounting
    - Delta table access
    - Efficient data processing with minimal data movement

Example Usage:
    >>> from core.settings import get_settings
    >>> from core.compute.types import SparkJobConfig, JobDefinition
    >>> 
    >>> settings = get_settings()
    >>> engine = FabricSparkEngine(settings.compute.fabric)
    >>> 
    >>> # Upload files for job execution
    >>> files = {
    ...     "etl_job.py": b"# Main ETL logic\nprint('Processing data')",
    ...     "utils.py": b"# Utility functions\ndef helper(): pass"
    ... }
    >>> upload_result = engine.upload_job_files(files, "/jobs/etl/")
    >>> 
    >>> # Create job definition
    >>> job_def = JobDefinition(
    ...     name="daily_etl",
    ...     main_file="/jobs/etl/etl_job.py",
    ...     additional_files=["/jobs/etl/utils.py"]
    ... )
    >>> definition_id = engine.create_job_definition("daily_etl", "/jobs/etl/etl_job.py", job_def)
    >>> 
    >>> # Execute job with parameters
    >>> job_id = engine.run_job_definition(
    ...     definition_id,
    ...     parameters={"date": "2024-01-15", "table": "sales"}
    ... )
    >>> 
    >>> # Monitor execution
    >>> status = engine.get_job_status(job_id)
    >>> logs = engine.get_job_logs(job_id)
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests
from urllib.parse import urljoin

from core.compute.engines.base import BaseSparkEngine

from core.compute.types import (
    JobResult,
    JobStatus,
    SparkJobConfig
)

# Import Fabric-specific types from local module
from .types import (
    SparkSessionConfig,
    NotebookConfig,
    JobDefinition,
    SessionInfo,
    StatementResult,
    SessionStatus,
    NotebookResult,
    JobDefinitionInfo,
    FileUploadResult,
    JobLogs
)
from core.utils.decorators import retry_with_backoff as retry
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.settings import FabricSettings


logger = logging.getLogger(__name__)


class FabricSparkEngine(BaseSparkEngine):
    """Spark engine implementation for Microsoft Fabric.
    
    This engine handles Spark job submission and monitoring using Fabric's
    REST API with native OneLake integration for file management and data processing.
    
    The Fabric implementation follows a 3-step process:
    1. Upload job files to OneLake (required)
    2. Create job definitions with file references
    3. Execute jobs with parameters and monitoring
    """
    
    def __init__(self, settings: 'FabricSettings'):
        """Initialize Fabric Spark engine.
        
        Args:
            settings: Fabric settings from configuration
            
        Raises:
            ValueError: If required Fabric settings are missing
        """
        if not settings.workspace_id:
            raise ValueError("Fabric workspace_id is required")
            
        self.settings = settings
        self.workspace_id = settings.workspace_id
        self.capacity_id = settings.capacity_id
        self.base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}"
        self._session = self._create_session()
        self._connection_info = {
            "platform": "fabric",
            "workspace_id": self.workspace_id,
            "capacity_id": self.capacity_id,
            "base_url": self.base_url
        }
        
        # Session management
        self._active_sessions: Dict[str, SessionInfo] = {}
        self._job_definitions: Dict[str, JobDefinition] = {}
        self._uploaded_files: Dict[str, str] = {}  # filename -> onelake_path
        
    def _create_session(self) -> requests.Session:
        """Create HTTP session with Fabric authentication."""
        session = requests.Session()
        
        # Get access token for Fabric using managed identity or service principal
        try:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            session.headers.update({
                "Authorization": f"Bearer {token.token}",
                "Content-Type": "application/json"
            })
        except Exception as e:
            logger.warning(f"Failed to authenticate with Fabric: {e}. Using mock authentication.")
            # In development, we might not have proper credentials
            session.headers.update({
                "Authorization": "Bearer mock-token",
                "Content-Type": "application/json"
            })
        
        return session
    
    # ============================================================================
    # BASE SPARK ENGINE METHODS - Batch Jobs
    # ============================================================================
    
    @retry(max_retries=3, initial_delay=1, exponential_base=2)
    def execute_spark_job(self, query: str, job_config: Optional[SparkJobConfig] = None,
                         context: Optional[Dict[str, Any]] = None) -> JobResult:
        """Execute a Spark SQL query and wait for completion."""
        # Submit job
        job_id = self.submit_batch_job(query, job_config, context)
        
        # Wait for completion
        start_time = time.time()
        timeout = job_config.timeout_seconds if job_config else 3600
        
        while time.time() - start_time < timeout:
            status = self.get_job_status(job_id)
            
            if status == JobStatus.SUCCESS:
                return JobResult(
                    job_id=job_id,
                    status=status,
                    start_time=start_time,
                    end_time=time.time()
                )
            elif status == JobStatus.FAILED:
                return JobResult(
                    job_id=job_id,
                    status=status,
                    start_time=start_time,
                    end_time=time.time(),
                    error="Job execution failed"
                )
            
            time.sleep(5)  # Poll every 5 seconds
        
        # Timeout reached
        return JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            start_time=start_time,
            error="Job timed out"
        )
    
   