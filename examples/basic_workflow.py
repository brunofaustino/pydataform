#!/usr/bin/env python
"""
Basic example of using PyDataform to run a Dataform workflow.

This example demonstrates:
1. Setting up a Dataform configuration
2. Creating a Dataform service
3. Running a workflow with different options
4. Monitoring workflow status
5. Handling workflow completion
"""

import os
import time
import logging
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))  # Add root directory to path
from dataform import DataformConfig, DataformService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("example")

def main():
    # Get configuration from environment variables (recommended for production)
    # You can also hardcode these values for testing
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
    
    logger.info(f"Using Dataform configuration: {config}")
    
    # Create Dataform service
    service = DataformService(config)
    
    # Example 1: Run a workflow with default settings
    logger.info("Example 1: Running a workflow with default settings")
    try:
        workflow = service.run_workflow()
        logger.info(f"Workflow started: {workflow.name}")
        logger.info(f"Initial state: {workflow.state}")
    except Exception as e:
        logger.error(f"Error running workflow: {e}")
    
    # Example 2: Run a workflow with custom parameters and wait for completion
    logger.info("\nExample 2: Running a workflow with custom parameters and waiting for completion")
    try:
        # Optional: Generate a unique execution ID (e.g., from a solver or other process)
        execution_id = f"example-{int(time.time())}"
        
        # Run workflow with custom parameters
        workflow = service.run_workflow(
            execution_id=execution_id,
            wait=True,  # Wait for completion
            timeout_seconds=7200,  # 2 hours timeout
            full_refresh=True  # Fully refresh incremental tables
        )
        
        # Check workflow status
        logger.info(f"Workflow completed: {workflow.name}")
        logger.info(f"Final state: {workflow.state}")
        logger.info(f"Duration: {workflow.duration_seconds:.2f} seconds")
        
        # Check if successful
        if workflow.is_successful:
            logger.info("Workflow completed successfully!")
        elif workflow.is_failed:
            logger.error("Workflow failed!")
        elif workflow.is_cancelled:
            logger.warning("Workflow was cancelled!")
    except Exception as e:
        logger.error(f"Error running workflow: {e}")
    
    # Example 3: List recent workflows
    logger.info("\nExample 3: Listing recent workflows")
    try:
        workflows = service.list_recent_workflows(limit=5)
        logger.info(f"Found {len(workflows)} recent workflows:")
        for wf in workflows:
            logger.info(f"  - {wf.name.split('/')[-1]}: {wf.state}")
    except Exception as e:
        logger.error(f"Error listing workflows: {e}")

if __name__ == "__main__":
    main() 