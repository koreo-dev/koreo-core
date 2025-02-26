import unittest

from koreo import cache
from koreo import result
from koreo.resource_function.prepare import prepare_resource_function
from koreo.resource_function.structure import ResourceFunction

from koreo.function_test import prepare
from koreo.function_test import run


class TestEndToEndResourceFunction(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        if cache.get_resource_from_cache(
            resource_class=ResourceFunction, cache_key="function-under-test"
        ):
            await cache.delete_from_cache(
                resource_class=ResourceFunction, cache_key="function-under-test"
            )

    async def asyncSetUp(self):
        if cache.get_resource_from_cache(
            resource_class=ResourceFunction, cache_key="function-under-test"
        ):
            cache.delete_from_cache
            return

        await cache.prepare_and_cache(
            resource_class=ResourceFunction,
            preparer=prepare_resource_function,
            metadata={"name": "function-under-test", "resourceVersion": "sam123"},
            spec={
                "apiConfig": {
                    "apiVersion": "test.koreo.dev/v1",
                    "kind": "TestResource",
                    "name": "=inputs.metadata.name + '-suffix'",
                    "namespace": "=inputs.metadata.namespace",
                },
                "preconditions": [
                    {
                        "assert": "=!has(inputs.preconditions.depSkip)",
                        "depSkip": {"message": "Input Validator Dep Skip"},
                    },
                    {
                        "assert": "=!has(inputs.preconditions.skip)",
                        "skip": {"message": "Input Validator Skip"},
                    },
                    {
                        "assert": "=!has(inputs.preconditions.permFail)",
                        "permFail": {"message": "Input Validator Perm Fail"},
                    },
                    {
                        "assert": "=!has(inputs.preconditions.retry)",
                        "retry": {"delay": 18, "message": "Input Validator Retry"},
                    },
                ],
                "resource": {
                    "apiVersion": "test.koreo.dev/v1",
                    "kind": "TestResource",
                    "metadata": "=inputs.metadata",
                    "spec": {
                        "subStructure": {
                            "int": 1,
                            "bool": True,
                            "list": [1, 2, 3],
                            "object": {"one": 1, "two": 2},
                        }
                    },
                },
                "create": {
                    "overlay": {
                        "spec": {
                            "subStructure": {
                                "createOnly": "value",
                            },
                        },
                    },
                },
                "postconditions": [
                    {
                        "assert": "=!has(resource.status.postconditions.depSkip)",
                        "depSkip": {"message": "Output Validator Dep Skip"},
                    },
                    {
                        "assert": "=!has(resource.status.postconditions.skip)",
                        "skip": {"message": "Output Validator Skip"},
                    },
                    {
                        "assert": "=!has(resource.status.postconditions.permFail)",
                        "permFail": {"message": "Output Validator Perm Fail"},
                    },
                    {
                        "assert": "=!has(resource.status.postconditions.retry)",
                        "retry": {"delay": 18, "message": "Output Validator Retry"},
                    },
                    {
                        "assert": "=has(resource.status.returnValue)",
                        "retry": {"delay": 1, "message": "Waiting for ready-state"},
                    },
                ],
                "return": {"value": "=resource.status.returnValue"},
            },
        )

    async def test_resource_function_fully(self):
        prepared_test = await prepare.prepare_function_test(
            cache_key="function-test:end-to-end",
            spec={
                "functionRef": {
                    "kind": "ResourceFunction",
                    "name": "function-under-test",
                },
                "inputs": {
                    "metadata": {
                        "name": "function-test:end-to-end",
                        "namespace": "unittests",
                    },
                },
                "testCases": [
                    {
                        "label": "Initial create",
                        "expectResource": {
                            "apiVersion": "test.koreo.dev/v1",
                            "kind": "TestResource",
                            "metadata": {
                                "name": "function-test:end-to-end-suffix",
                                "namespace": "unittests",
                            },
                            "spec": {
                                "subStructure": {
                                    "int": 1,
                                    "bool": True,
                                    "list": [1, 2, 3],
                                    "object": {"one": 1, "two": 2},
                                    "createOnly": "value",
                                }
                            },
                        },
                    },
                    {
                        "label": "[variant] Rename causes create",
                        "variant": True,
                        "inputOverrides": {
                            "metadata": {"name": "pumpkins"},
                        },
                        "expectResource": {
                            "apiVersion": "test.koreo.dev/v1",
                            "kind": "TestResource",
                            "metadata": {
                                "name": "pumpkins-suffix",
                                "namespace": "unittests",
                            },
                            "spec": {
                                "subStructure": {
                                    "int": 1,
                                    "bool": True,
                                    "list": [1, 2, 3],
                                    "object": {"one": 1, "two": 2},
                                }
                            },
                        },
                    },
                    {
                        "label": "Setting status overlay returns value",
                        "overlayResource": {
                            "status": {"returnValue": "controller-set-value"}
                        },
                        "expectReturn": {"value": "controller-set-value"},
                    },
                    {
                        "label": "Non-variant status overlay persists",
                        "expectReturn": {"value": "controller-set-value"},
                    },
                    {
                        "label": "[variant] Status overlay failure",
                        "variant": True,
                        "overlayResource": {
                            "status": {
                                "returnValue": "variant-value",
                                "postconditions": {"permFail": True},
                            }
                        },
                        "expectOutcome": {"permFail": {"message": "Output Validator"}},
                    },
                    {
                        "label": "Variant status overlay did not persist",
                        "expectReturn": {"value": "controller-set-value"},
                    },
                    {
                        "label": "[variant] Input overlay triggers preconditions",
                        "variant": True,
                        "inputOverrides": {
                            "preconditions": {"permFail": True},
                        },
                        "expectOutcome": {"permFail": {"message": "Input Validator"}},
                    },
                    {
                        "label": "variant input overlay did not persist",
                        "expectReturn": {"value": "controller-set-value"},
                    },
                ],
            },
        )

        if not result.is_unwrapped_ok(prepared_test):
            raise self.failureException(
                f"Failed to prepare function-test {prepared_test}!"
            )

        function_test, _ = prepared_test

        test_result = await run.run_function_test(
            location="unit-test:end-to-end", function_test=function_test
        )

        self.assertGreater(len(test_result.test_results), 0, "No test results")
        for idx, test_case_result in enumerate(test_result.test_results):
            if test_case_result.test_pass:
                continue

            if test_case_result.message:
                message = f"{test_case_result.message}"
            elif test_case_result.differences:
                message = f"{test_case_result.differences}"
            else:
                message = f"{test_case_result.outcome}"

            test_name = (
                test_case_result.label if test_case_result.label else f"Test {idx}"
            )

            raise self.failureException(f"{test_name} Failure: {message}")
