import random
import string
import unittest

from koreo.function import registry


def _name_generator(prefix: str):
    return f"{prefix}-{"".join(random.choices(string.ascii_lowercase, k=15))}"


class TestRegistry(unittest.TestCase):
    def setUp(self):
        registry._reset_registry()

    def test_roundtrip(self):
        workflow_name = _name_generator("workflow")
        functions = [_name_generator("function") for _ in range(random.randint(5, 25))]

        registry.index_workflow_functions(workflow=workflow_name, functions=functions)

        for function in functions:
            self.assertListEqual(
                [workflow_name], registry.get_function_workflows(function)
            )

    def test_remove_usage(self):
        workflow_name_one = _name_generator("workflow")
        workflow_name_two = _name_generator("workflow")

        # Used throughout
        common_functions = [
            _name_generator("function") for _ in range(random.randint(2, 5))
        ]

        # Used throughout
        workflow_two_functions = common_functions + [
            _name_generator("function") for _ in range(random.randint(2, 5))
        ]

        # Used the original set
        workflow_one_original_functions = common_functions + [
            _name_generator("function") for _ in range(random.randint(2, 5))
        ]

        # Index everything
        registry.index_workflow_functions(
            workflow=workflow_name_one, functions=workflow_one_original_functions
        )

        registry.index_workflow_functions(
            workflow=workflow_name_two, functions=workflow_two_functions
        )

        # Upate One's functions
        workflow_one_new_functions = common_functions + [
            _name_generator("function") for _ in range(random.randint(2, 5))
        ]
        registry.index_workflow_functions(
            workflow=workflow_name_one, functions=workflow_one_new_functions
        )

        # Common is always used and should contain both workflows
        for function in common_functions:
            workflows = registry.get_function_workflows(function=function)
            self.assertIn(workflow_name_one, workflows)
            self.assertIn(workflow_name_two, workflows)

        # Workflow Two's unique functions should only it.
        for function in set(workflow_two_functions).difference(common_functions):
            workflows = registry.get_function_workflows(function=function)
            self.assertListEqual([workflow_name_two], workflows)

        # Workflow One's unique _original_ functions no longer reference it.
        for function in set(workflow_one_original_functions).difference(
            common_functions
        ):
            workflows = registry.get_function_workflows(function=function)
            self.assertNotIn(workflow_name_one, workflows)

        # Workflow One's unique _new_ functions should only reference it.
        for function in set(workflow_one_new_functions).difference(common_functions):
            workflows = registry.get_function_workflows(function=function)
            self.assertIn(workflow_name_one, workflows)
