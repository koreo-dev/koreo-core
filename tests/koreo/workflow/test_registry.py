import random
import string
import unittest

from koreo.workflow import registry


def _name_generator(prefix: str):
    return f"{prefix}-{"".join(random.choices(string.ascii_lowercase, k=15))}"


class TestRegistry(unittest.TestCase):
    def setUp(self):
        registry._reset_registry()

    def test_roundtrip(self):
        custom_crd_name = _name_generator("crd")
        workflow_name_one = _name_generator("workflow")
        workflow_name_two = _name_generator("workflow")

        registry.index_workflow_custom_crd(
            workflow=workflow_name_one, custom_crd=custom_crd_name
        )
        registry.index_workflow_custom_crd(
            workflow=workflow_name_two, custom_crd=custom_crd_name
        )

        workflows = registry.get_custom_crd_workflows(custom_crd=custom_crd_name)
        self.assertIn(workflow_name_one, workflows)
        self.assertIn(workflow_name_two, workflows)

    def test_no_change_usage_reindex(self):
        workflow_name = _name_generator("workflow")

        custom_crd_name = _name_generator("crd")

        registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name
        )
        registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name
        )

        self.assertEqual(
            [workflow_name], registry.get_custom_crd_workflows(custom_crd_name)
        )

    def test_change_usage(self):
        workflow_name = _name_generator("workflow")

        custom_crd_name_one = _name_generator("crd")
        custom_crd_name_two = _name_generator("crd")

        registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name_one
        )
        registry.index_workflow_custom_crd(
            workflow=workflow_name, custom_crd=custom_crd_name_two
        )

        self.assertNotIn(
            workflow_name, registry.get_custom_crd_workflows(custom_crd_name_one)
        )
        self.assertIn(
            workflow_name, registry.get_custom_crd_workflows(custom_crd_name_two)
        )
