# PyDataform

<img src="PyDataform.png" alt="PyDataform Logo" width="200"/>

A Python library for simplifying Google Cloud Dataform operations. This library provides a high-level interface to manage Dataform workflows, making it easier to compile, run, and monitor Dataform operations in your Python applications.

## Features

- Simple configuration management for Dataform projects
- Easy workflow compilation and execution
- Comprehensive workflow status monitoring
- Support for parameterized workflows
- Built-in waiting and timeout mechanisms
- Clean and intuitive API

## Installation

```bash
pip install pydataform
```

## Authentication

The library uses Google Cloud's default authentication mechanism. There are several ways to authenticate:

1. **Service Account (Recommended for Production)**
   ```bash
   # Set the path to your service account key file
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

2. **User Credentials (Development)**
   ```bash
   # Login with your Google account
   gcloud auth application-default login
   ```

3. **Compute Engine/Cloud Run (Automatic)**
   If running on Google Cloud Platform services, authentication is handled automatically.

For more information about authentication, see the [Google Cloud Authentication documentation](https://cloud.google.com/docs/authentication).

## Quick Start

```python
from pydataform import DataformConfig, DataformService

# Configure your Dataform project
config = DataformConfig(
    project_id="your-project-id",
    location="us-central1",
    repo_name="your-repo-name",
    git_branch="main"
)

# Create a Dataform service instance
service = DataformService(config)

# Run a workflow and wait for completion
workflow = service.run_workflow(
    execution_id="optional-execution-id",
    wait=True,
    timeout_seconds=3600,
    full_refresh=True
)

# Check workflow status
print(f"Workflow state: {workflow.state}")
print(f"Duration: {workflow.duration_seconds} seconds")
```

## Usage

### Configuration

```python
config = DataformConfig(
    project_id="your-project-id",
    location="us-central1",  # optional, defaults to us-central1
    repo_name="your-repo-name",
    git_branch="main"  # optional, defaults to main
)
```

### Running Workflows

```python
# Run with default settings
workflow = service.run_workflow()

# Run with custom parameters
workflow = service.run_workflow(
    execution_id="custom-id",
    wait=True,
    timeout_seconds=7200,
    full_refresh=False
)
```

### Monitoring Workflows

```python
# Check workflow status
print(workflow.state)  # RUNNING, SUCCEEDED, FAILED, etc.
print(workflow.is_complete)  # True/False
print(workflow.is_successful)  # True/False
print(workflow.duration_seconds)  # 123.45

# Wait for completion
workflow.wait_for_completion(
    poll_interval_seconds=30,
    timeout_seconds=3600
)
```

### Listing Recent Workflows

```python
# Get recent workflows
workflows = service.list_recent_workflows(limit=10)
for workflow in workflows:
    print(f"{workflow.name}: {workflow.state}")
```

## Examples

### Environment Setup

Before running the examples, set up your environment variables:

```bash
# GCP Project Configuration
export GCP_PROJECT_ID="your-project-id"        # Your GCP project ID
export GCP_LOCATION="us-central1"              # Your GCP region
export DATAFORM_REPO="your-repo-name"          # Your GCP Dataform repository name
export DATAFORM_BRANCH="test"                  # Your GCP Dataform repository branch
```

These variables are used by the example scripts and can be overridden for different environments (development, production, etc.).

---

The library includes two example scripts demonstrating different usage patterns:

### Basic Workflow Example

The basic example (`examples/basic_workflow.py`) demonstrates simple workflow execution:

```python
# Example 1: Run a workflow with default settings
workflow = service.run_workflow()
print(f"Initial state: {workflow.state}")

# Example 2: Run a workflow with custom parameters and wait for completion
workflow = service.run_workflow(
    execution_id="custom-id",
    wait=True,
    timeout_seconds=3600,
    full_refresh=True
)
print(f"Final state: {workflow.state}")
print(f"Duration: {workflow.duration_seconds:.2f} seconds")

# Example 3: List recent workflows
workflows = service.list_recent_workflows(limit=5)
print(f"Found {len(workflows)} recent workflows:")
for wf in workflows:
    print(f"  - {wf.name.split('/')[-1]}: {wf.state}")
```

### Advanced Workflow Example

The advanced example (`examples/advanced_workflow.py`) demonstrates more complex workflow management:

```python
# Create a workflow manager for handling multiple workflows
manager = WorkflowManager(
    config=config,
    max_retries=2,
    retry_delay_seconds=30,
    max_concurrent_workflows=2
)

# Define callbacks for workflow events
def on_workflow_start(workflow):
    print(f"Workflow started: {workflow.name}")

def on_workflow_complete(workflow):
    print(f"Workflow completed: {workflow.name} with state: {workflow.state}")
    if workflow.is_successful:
        print(f"Duration: {workflow.duration_seconds:.2f} seconds")

def on_workflow_error(workflow, error):
    print(f"Workflow error: {error}")

# Run multiple workflows with callbacks
workflow_id1 = manager.run_workflow(
    execution_id="example-workflow-1",
    wait=False,
    on_start=on_workflow_start,
    on_complete=on_workflow_complete,
    on_error=on_workflow_error
)

# Monitor active workflows
workflows = manager.get_all_workflows()
print(f"Active workflows: {len(workflows)}")
for wf in workflows:
    print(f"  - {wf.name.split('/')[-1]}: {wf.state}")
```

The advanced example includes features like:
- Concurrent workflow execution
- Automatic retry on failure
- Event callbacks for workflow lifecycle
- Active workflow monitoring
- Graceful shutdown handling

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

If you encounter any issues or have questions, please file an issue on the GitHub repository. 