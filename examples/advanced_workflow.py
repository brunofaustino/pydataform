#!/usr/bin/env python
"""
Advanced example of using PyDataform for complex workflow management.

This example demonstrates:
1. Setting up a Dataform configuration with custom settings
2. Implementing a workflow manager class
3. Handling workflow lifecycle events
4. Implementing retry logic for failed workflows
5. Monitoring workflow progress with custom callbacks
6. Managing multiple workflows in parallel
"""

import os
import time
import logging
import threading
from typing import Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))  # Add root directory to path
from dataform import DataformConfig, DataformService, DataformWorkflow

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("advanced_example")

class WorkflowManager:
    """Manages multiple Dataform workflows with advanced features."""
    
    def __init__(self, config: DataformConfig, max_retries: int = 3, 
                 retry_delay_seconds: int = 60, max_concurrent_workflows: int = 3):
        """Initialize the workflow manager.
        
        Args:
            config: Dataform configuration
            max_retries: Maximum number of retry attempts for failed workflows
            retry_delay_seconds: Delay between retry attempts
            max_concurrent_workflows: Maximum number of workflows to run in parallel
        """
        self.config = config
        self.service = DataformService(config)
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.max_concurrent_workflows = max_concurrent_workflows
        self.active_workflows: Dict[str, DataformWorkflow] = {}
        self.workflow_callbacks: Dict[str, List[Callable]] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_workflows)
        self.lock = threading.Lock()
        self._monitor_thread = threading.Thread(target=self._monitor_workflows, daemon=True)
        self._monitor_thread.start()
    
    def _monitor_workflows(self):
        """Monitor active workflows and handle completion."""
        while True:
            with self.lock:
                completed = []
                for execution_id, workflow in self.active_workflows.items():
                    try:
                        workflow.refresh()
                        if workflow.is_complete:
                            completed.append(execution_id)
                            callbacks = self.workflow_callbacks.get(execution_id)
                            if callbacks and callbacks[1]:  # on_complete callback
                                callbacks[1](workflow)
                    except Exception as e:
                        logger.error(f"Error monitoring workflow {execution_id}: {e}")
                
                # Remove completed workflows
                for execution_id in completed:
                    if execution_id in self.active_workflows:
                        del self.active_workflows[execution_id]
                    if execution_id in self.workflow_callbacks:
                        del self.workflow_callbacks[execution_id]
            
            time.sleep(5)  # Check every 5 seconds
    
    def _run_workflow_with_retry(self, 
                                execution_id: str,
                                wait: bool,
                                timeout_seconds: int,
                                full_refresh: bool) -> None:
        """Internal method to run workflow with retry logic."""
        retries = 0
        last_error = None
        workflow = None
        
        while retries <= self.max_retries:
            try:
                # Run the workflow
                workflow = self.service.run_workflow(
                    execution_id=execution_id,
                    wait=False,  # Always use non-blocking mode
                    timeout_seconds=timeout_seconds,
                    full_refresh=full_refresh
                )
                
                # Store active workflow
                with self.lock:
                    self.active_workflows[execution_id] = workflow
                
                # Call on_start callback
                callbacks = self.workflow_callbacks.get(execution_id, [])
                if callbacks and callbacks[0]:
                    callbacks[0](workflow)
                
                # If wait is requested, wait for completion
                if wait:
                    workflow.wait_for_completion(timeout_seconds=timeout_seconds)
                
                # Success - exit retry loop
                break
                
            except Exception as e:
                last_error = e
                retries += 1
                
                # Call on_error callback
                callbacks = self.workflow_callbacks.get(execution_id, [])
                if callbacks and callbacks[2]:
                    callbacks[2](workflow, e)
                
                # Log error
                logger.error(f"Workflow {execution_id} failed (attempt {retries}/{self.max_retries+1}): {e}")
                
                # Wait before retry
                if retries <= self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay_seconds} seconds...")
                    time.sleep(self.retry_delay_seconds)
    
    def shutdown(self) -> None:
        """Shutdown the workflow manager."""
        # Wait for active workflows to complete
        timeout = time.time() + 60  # Wait up to 60 seconds
        while time.time() < timeout and self.active_workflows:
            time.sleep(1)
        
        self.executor.shutdown(wait=True)

    def run_workflow(self, 
                    execution_id: Optional[str] = None,
                    wait: bool = True,
                    timeout_seconds: int = 3600,
                    full_refresh: bool = True,
                    on_start: Optional[Callable[[DataformWorkflow], None]] = None,
                    on_complete: Optional[Callable[[DataformWorkflow], None]] = None,
                    on_error: Optional[Callable[[DataformWorkflow, Exception], None]] = None) -> str:
        """Run a workflow with callbacks and retry logic.
        
        Args:
            execution_id: Optional execution ID
            wait: Whether to wait for completion
            timeout_seconds: Maximum time to wait
            full_refresh: Whether to fully refresh incremental tables
            on_start: Callback when workflow starts
            on_complete: Callback when workflow completes
            on_error: Callback when workflow errors
            
        Returns:
            Workflow ID
        """
        # Generate execution ID if not provided
        if not execution_id:
            execution_id = f"workflow-{int(time.time())}-{id(self)}"
        
        # Store callbacks
        self.workflow_callbacks[execution_id] = [on_start, on_complete, on_error]
        
        # Submit workflow to thread pool
        self.executor.submit(
            self._run_workflow_with_retry,
            execution_id,
            wait,
            timeout_seconds,
            full_refresh
        )
        
        return execution_id
    
    def get_workflow(self, execution_id: str) -> Optional[DataformWorkflow]:
        """Get a workflow by execution ID."""
        with self.lock:
            return self.active_workflows.get(execution_id)
    
    def get_all_workflows(self) -> List[DataformWorkflow]:
        """Get all active workflows."""
        with self.lock:
            return list(self.active_workflows.values())

def main():
    # Get configuration from environment variables
    project_id = os.environ.get("GCP_PROJECT_ID", "your-project-id")
    location = os.environ.get("GCP_LOCATION", "us-central1")
    repo_name = os.environ.get("DATAFORM_REPO", "your-repo-name")
    git_branch = os.environ.get("DATAFORM_BRANCH", "main")
    
    # Create Dataform configuration
    config = DataformConfig(
        project_id=project_id,
        location=location,
        repo_name=repo_name,
        git_branch=git_branch
    )
    
    # Create workflow manager
    manager = WorkflowManager(
        config=config,
        max_retries=2,
        retry_delay_seconds=30,
        max_concurrent_workflows=2
    )
    
    # Define callbacks
    def on_workflow_start(workflow):
        logger.info(f"Workflow started: {workflow.name}")
    
    def on_workflow_complete(workflow):
        logger.info(f"Workflow completed: {workflow.name} with state: {workflow.state}")
        if workflow.is_successful:
            logger.info(f"Duration: {workflow.duration_seconds:.2f} seconds")
    
    def on_workflow_error(workflow, error):
        logger.error(f"Workflow error: {error}")
    
    # Run multiple workflows
    workflow_ids = []
    
    # Workflow 1: Standard workflow
    workflow_id1 = manager.run_workflow(
        execution_id="example-workflow-1",
        wait=False,  # Don't wait for completion
        on_start=on_workflow_start,
        on_complete=on_workflow_complete,
        on_error=on_workflow_error
    )
    workflow_ids.append(workflow_id1)
    
    # Workflow 2: Workflow with custom parameters
    workflow_id2 = manager.run_workflow(
        execution_id="example-workflow-2",
        wait=False,
        full_refresh=True,
        on_start=on_workflow_start,
        on_complete=on_workflow_complete,
        on_error=on_workflow_error
    )
    workflow_ids.append(workflow_id2)
    
    # Wait a moment for workflows to be registered
    time.sleep(5)
    
    # Monitor workflows
    logger.info("Monitoring workflows...")
    for _ in range(5):  # Check 5 times
        workflows = manager.get_all_workflows()
        logger.info(f"Active workflows: {len(workflows)}")
        for wf in workflows:
            logger.info(f"  - {wf.name.split('/')[-1]}: {wf.state}")
        time.sleep(10)  # Wait 10 seconds between checks
    
    # Wait for workflows to complete
    logger.info("Waiting for workflows to complete...")
    time.sleep(30)  # Wait 30 seconds
    
    # Shutdown manager
    logger.info("Shutting down workflow manager...")
    manager.shutdown()
    
    logger.info("Example completed!")


if __name__ == "__main__":
    main() 