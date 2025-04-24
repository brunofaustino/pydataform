from typing import Dict, Optional, Any, Union, List
from google.cloud import dataform_v1beta1
import time
import logging

# Configure logger
logger = logging.getLogger("pydataform")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

class DataformConfig:
    """Configuration for Dataform API connections and resources."""
    
    def __init__(self, 
                 project_id: str, 
                 location: str = 'us-central1',
                 repo_name: str = 'inland-nam-rcs-df-dev-bg',
                 git_branch: str = 'test'):
        """Initialize Dataform configuration.
        
        Args:
            project_id: GCP project ID
            location: GCP region (default: 'us-central1')
            repo_name: Name of the Dataform repository
            git_branch: Git branch to use (default: 'test')
        """
        self.project_id = project_id
        self.location = location
        self.repo_name = repo_name
        self.git_branch = git_branch
        
    @property
    def repo_uri(self) -> str:
        """Get the fully qualified repository URI."""
        return f'projects/{self.project_id}/locations/{self.location}/repositories/{self.repo_name}'
    
    def __str__(self) -> str:
        return f"DataformConfig(project_id={self.project_id}, repo_name={self.repo_name}, branch={self.git_branch})"


class DataformWorkflow:
    """Represents a single Dataform workflow execution instance."""
    
    def __init__(self, client: dataform_v1beta1.DataformClient, workflow_invocation: dataform_v1beta1.WorkflowInvocation):
        """Initialize a workflow instance.
        
        Args:
            client: The Dataform API client
            workflow_invocation: The workflow invocation response from the API
        """
        self._client = client
        self._workflow_invocation = workflow_invocation
        
    @property
    def name(self) -> str:
        """Get the workflow invocation name."""
        return self._workflow_invocation.name
    
    @property
    def compilation_result(self) -> str:
        """Get the compilation result name used for this workflow."""
        return self._workflow_invocation.compilation_result
    
    @property
    def raw_state(self) -> int:
        """Get the raw numeric state value."""
        self.refresh()
        return self._workflow_invocation.state
    
    @property
    def state(self) -> str:
        """Get the current state as a string."""
        self.refresh()
        return dataform_v1beta1.WorkflowInvocation.State(self._workflow_invocation.state).name
    
    @property
    def start_time(self) -> Optional[Dict[str, int]]:
        """Get the workflow start time, if available."""
        if not hasattr(self._workflow_invocation, 'invocation_timing') or not self._workflow_invocation.invocation_timing:
            return None
        if not hasattr(self._workflow_invocation.invocation_timing, 'start_time') or not self._workflow_invocation.invocation_timing.start_time:
            return None
        
        start = self._workflow_invocation.invocation_timing.start_time
        return {
            'seconds': start.seconds,
            'nanos': start.nanos
        }
    
    @property
    def end_time(self) -> Optional[Dict[str, int]]:
        """Get the workflow end time, if available."""
        if not hasattr(self._workflow_invocation, 'invocation_timing') or not self._workflow_invocation.invocation_timing:
            return None
        if not hasattr(self._workflow_invocation.invocation_timing, 'end_time') or not self._workflow_invocation.invocation_timing.end_time:
            return None
        
        end = self._workflow_invocation.invocation_timing.end_time
        return {
            'seconds': end.seconds,
            'nanos': end.nanos
        }
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get the workflow duration in seconds, if available."""
        start = self.start_time
        end = self.end_time
        
        if not start or not end:
            return None
        
        return (end['seconds'] - start['seconds']) + ((end['nanos'] - start['nanos']) / 1e9)
    
    @property
    def is_complete(self) -> bool:
        """Check if workflow has completed (successfully or not)."""
        self.refresh()
        terminal_states = [
            dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED,
            dataform_v1beta1.WorkflowInvocation.State.CANCELLED,
            dataform_v1beta1.WorkflowInvocation.State.FAILED
        ]
        return self._workflow_invocation.state in terminal_states
    
    @property
    def is_successful(self) -> bool:
        """Check if workflow completed successfully."""
        self.refresh()
        return self._workflow_invocation.state == dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED
    
    @property
    def is_failed(self) -> bool:
        """Check if workflow failed."""
        self.refresh()
        return self._workflow_invocation.state == dataform_v1beta1.WorkflowInvocation.State.FAILED
    
    @property
    def is_cancelled(self) -> bool:
        """Check if workflow was cancelled."""
        self.refresh()
        return self._workflow_invocation.state == dataform_v1beta1.WorkflowInvocation.State.CANCELLED
    
    @property
    def is_running(self) -> bool:
        """Check if workflow is currently running."""
        self.refresh()
        return self._workflow_invocation.state == dataform_v1beta1.WorkflowInvocation.State.RUNNING
    
    def refresh(self) -> 'DataformWorkflow':
        """Update the workflow status from the API.
        
        Returns:
            self: For method chaining
        """
        self._workflow_invocation = self._client.get_workflow_invocation(name=self.name)
        return self
    
    def wait_for_completion(self, poll_interval_seconds: int = 30, timeout_seconds: int = 3600) -> 'DataformWorkflow':
        """Wait for the workflow to complete.
        
        Args:
            poll_interval_seconds: How frequently to check status (default: 30s)
            timeout_seconds: Maximum time to wait (default: 1 hour)
        
        Returns:
            self: For method chaining
            
        Raises:
            TimeoutError: If the workflow doesn't complete within the timeout period
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            self.refresh()
            
            if self.is_complete:
                return self

            logger.info(f"Execution not completed yet. Waiting {poll_interval_seconds} seconds before checking again...")
            time.sleep(poll_interval_seconds)
            
        raise TimeoutError(f"Workflow did not complete within {timeout_seconds} seconds")
    
    def __str__(self) -> str:
        return f"DataformWorkflow(name={self.name.split('/')[-1]}, state={self.state})"
    
    def __repr__(self) -> str:
        return self.__str__()


class DataformService:
    """Service for managing Dataform operations."""
    
    def __init__(self, config: DataformConfig):
        """Initialize the Dataform service.
        
        Args:
            config: Dataform configuration
        """
        self.config = config
        self.client = dataform_v1beta1.DataformClient()
        self.latest_workflow = None
    
    def compile(self, params: Optional[Dict[str, str]] = None, schema_suffix: Optional[str] = None) -> str:
        """Compile Dataform code.
        
        Args:
            params: Dictionary of parameters to pass to the compilation
            schema_suffix: Optional schema suffix for compilation
            
        Returns:
            The name of the compilation result
        """
        # Initialize compilation result
        compilation_result = dataform_v1beta1.CompilationResult()
        compilation_result.git_commitish = self.config.git_branch
        
        # Set parameters if provided
        if params:
            compilation_result.code_compilation_config.vars = params
            
        # Set schema suffix if provided
        if schema_suffix:
            compilation_result.code_compilation_config.schema_suffix = schema_suffix
        
        # Create request
        request = dataform_v1beta1.CreateCompilationResultRequest(
            parent=self.config.repo_uri,
            compilation_result=compilation_result,
        )
        
        # Send request
        response = self.client.create_compilation_result(request=request)
        
        return response.name
    
    def create_workflow(self, 
                        compilation_name: str, 
                        full_refresh: bool = True,
                        include_dependencies: bool = True,
                        include_dependents: bool = True) -> DataformWorkflow:
        """Create and return a new workflow.
        
        Args:
            compilation_name: Name of the compilation result
            full_refresh: Whether to fully refresh incremental tables (default: True)
            include_dependencies: Whether to include transitive dependencies (default: True)
            include_dependents: Whether to include transitive dependents (default: True)
            
        Returns:
            A DataformWorkflow object representing the created workflow
        """
        workflow_invocation = self._invoke_workflow(
            compilation_name, 
            full_refresh, 
            include_dependencies, 
            include_dependents
        )
        workflow = DataformWorkflow(self.client, workflow_invocation)
        self.latest_workflow = workflow
        return workflow
    
    def get_workflow(self, workflow_name: str) -> DataformWorkflow:
        """Get an existing workflow by name.
        
        Args:
            workflow_name: The name of the workflow invocation
            
        Returns:
            A DataformWorkflow object for the specified workflow
        """
        workflow_invocation = self.client.get_workflow_invocation(name=workflow_name)
        return DataformWorkflow(self.client, workflow_invocation)
    
    def run_workflow(self, 
                execution_id: Optional[str] = None, 
                wait: bool = False,
                timeout_seconds: int = 3600,
                full_refresh: bool = True) -> DataformWorkflow:
        """Run complete workflow process and return workflow.
        
        Args:
            execution_id: UUID from solver execution (optional)
            wait: Whether to wait for the workflow to complete
            timeout_seconds: Maximum time to wait if waiting for completion
            full_refresh: Whether to fully refresh incremental tables
            
        Returns:
            A DataformWorkflow object representing the created workflow
        """
        # Set up params
        params = {}
        if execution_id:
            params["defaultExecutionId"] = execution_id
        
        # Compile the code
        compilation_name = self.compile(params)
        
        # Create and invoke the workflow
        workflow = self.create_workflow(compilation_name, full_refresh)
        
        # Wait for completion if requested
        if wait:
            workflow.wait_for_completion(timeout_seconds=timeout_seconds)
            
        return workflow
    
    def list_recent_workflows(self, limit: int = 10) -> List[DataformWorkflow]:
        """List recent workflow invocations.
        
        Args:
            limit: Maximum number of workflows to return
            
        Returns:
            List of DataformWorkflow objects
        """
        request = dataform_v1beta1.ListWorkflowInvocationsRequest(
            parent=self.config.repo_uri,
            page_size=limit
        )
        
        response = self.client.list_workflow_invocations(request=request)
        workflows = []
        
        for workflow_invocation in response:
            workflows.append(DataformWorkflow(self.client, workflow_invocation))
            
        return workflows
    
    def _invoke_workflow(self, 
                         compilation_name: str, 
                         full_refresh: bool,
                         include_dependencies: bool,
                         include_dependents: bool) -> dataform_v1beta1.WorkflowInvocation:
        """Internal method to invoke a workflow.
        
        Args:
            compilation_name: Name of the compilation result
            full_refresh: Whether to fully refresh incremental tables
            include_dependencies: Whether to include transitive dependencies
            include_dependents: Whether to include transitive dependents
            
        Returns:
            The workflow invocation response from the API
        """
        # Configure invocation
        invocation_config = dataform_v1beta1.InvocationConfig(
            fully_refresh_incremental_tables_enabled=full_refresh,
            transitive_dependencies_included=include_dependencies,
            transitive_dependents_included=include_dependents
        )
        
        workflow_invocation = dataform_v1beta1.WorkflowInvocation(
            compilation_result=compilation_name,
            invocation_config=invocation_config,
        )
        
        # Create request
        request = dataform_v1beta1.CreateWorkflowInvocationRequest(
            parent=self.config.repo_uri,
            workflow_invocation=workflow_invocation,
        )
        
        # Send request
        return self.client.create_workflow_invocation(request=request)
