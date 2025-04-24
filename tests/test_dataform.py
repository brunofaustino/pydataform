import unittest
from unittest.mock import Mock, patch
from pydataform import DataformConfig, DataformService, DataformWorkflow
from google.cloud import dataform_v1beta1

class TestDataformConfig(unittest.TestCase):
    def test_init(self):
        config = DataformConfig(
            project_id="test-project",
            location="us-central1",
            repo_name="test-repo",
            git_branch="main"
        )
        self.assertEqual(config.project_id, "test-project")
        self.assertEqual(config.location, "us-central1")
        self.assertEqual(config.repo_name, "test-repo")
        self.assertEqual(config.git_branch, "main")
        
    def test_repo_uri(self):
        config = DataformConfig(
            project_id="test-project",
            location="us-central1",
            repo_name="test-repo"
        )
        expected_uri = "projects/test-project/locations/us-central1/repositories/test-repo"
        self.assertEqual(config.repo_uri, expected_uri)

class TestDataformWorkflow(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.mock_invocation = Mock()
        self.workflow = DataformWorkflow(self.mock_client, self.mock_invocation)
        
    def test_state_property(self):
        self.mock_invocation.state = dataform_v1beta1.WorkflowInvocation.State.RUNNING
        self.assertEqual(self.workflow.state, "RUNNING")
        
    def test_is_complete_property(self):
        self.mock_invocation.state = dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED
        self.assertTrue(self.workflow.is_complete)
        
        self.mock_invocation.state = dataform_v1beta1.WorkflowInvocation.State.RUNNING
        self.assertFalse(self.workflow.is_complete)

class TestDataformService(unittest.TestCase):
    def setUp(self):
        self.config = DataformConfig(
            project_id="test-project",
            location="us-central1",
            repo_name="test-repo"
        )
        self.service = DataformService(self.config)
        
    @patch('google.cloud.dataform_v1beta1.DataformClient')
    def test_compile(self, mock_client):
        mock_client.return_value.create_compilation_result.return_value.name = "test-compilation"
        result = self.service.compile()
        self.assertEqual(result, "test-compilation")
        
    @patch('google.cloud.dataform_v1beta1.DataformClient')
    def test_run_workflow(self, mock_client):
        mock_client.return_value.create_compilation_result.return_value.name = "test-compilation"
        mock_client.return_value.create_workflow_invocation.return_value = Mock()
        
        workflow = self.service.run_workflow()
        self.assertIsInstance(workflow, DataformWorkflow)

if __name__ == '__main__':
    unittest.main() 