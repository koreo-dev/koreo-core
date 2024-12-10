import unittest
import json

import celpy
import yaml

from koreo.function import prepare
from koreo.result import PermFail, is_unwrapped_ok

_functions = {}

with open("crd/sample-function.yaml", "r") as raw_yamls:
    yamls = yaml.load_all(raw_yamls, Loader=yaml.Loader)
    for function_yaml in yamls:
        if function_yaml.get("kind") != "Function":
            continue
        metadata = function_yaml.get("metadata", {})
        key = f"{metadata.get('name')}"
        _functions[key] = function_yaml


class TestFunctionErrors(unittest.IsolatedAsyncioTestCase):
    async def test_no_spec(self):
        prepared = await prepare.prepare_function(
            cache_key="empty.v1", spec=_functions["empty.v1"].get("spec")
        )
        self.assertIsInstance(prepared, PermFail)


class TestFunctionInputValidation(unittest.IsolatedAsyncioTestCase):
    async def test_no_validators(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-tests.v1",
            spec=_functions["outcome-tests.v1"].get("spec", {}),
        )
        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared
        self.assertIsNone(prepared_function.input_validators)

    async def test_no_problems(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        result = prepared_function.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 6, "optional_skip_number": 3333})}
        )

        self.assertFalse(result)

    async def test_skip(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 4})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Skip", result.get("type"))
        self.assertIn("too small", result.get("message"))

    async def test_optional_perm_fail(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 8, "optional_number": 250})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("PermFail", result.get("type"))
        self.assertIn("not equal", result.get("message"))

    async def test_retry_delay(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 15})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Retry", result.get("type"))
        self.assertEqual(90, result.get("delay"))
        self.assertIn("too large", result.get("message"))

    async def test_multiple_issues(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        result = prepared_function.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 0, "optional_retry_number": 7})}
        )

        self.assertEqual(3, len(result))

    async def test_dynamic_message(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.input_validators.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {"number": 8, "value_for_message": "set within message"}
                )
            }
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Skip", result.get("type"))
        self.assertEqual(
            'value for message ("set within message") was set.', result.get("message")
        )


class TestFunctionOutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def test_none_defined(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        self.assertIsNone(prepared_function.outcome.tests)

    async def test_optional_perm_fail(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-tests.v1",
            spec=_functions["outcome-tests.v1"].get("spec", {}),
        )
        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared
        cel_result = prepared_function.outcome.tests.evaluate(
            {"inputs": celpy.json_to_cel({"number": 5, "optional_number": 250})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("PermFail", result.get("type"))
        self.assertIn("not equal", result.get("message"))

    async def test_retry_delay(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-tests.v1",
            spec=_functions["outcome-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.outcome.tests.evaluate(
            {"inputs": celpy.json_to_cel({"number": 15})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Retry", result.get("type"))
        self.assertEqual(90, result.get("delay"))
        self.assertIn("too large", result.get("message"))

    async def test_multiple_issues(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-tests.v1",
            spec=_functions["outcome-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        result = prepared_function.outcome.tests.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {"number": 20, "optional_number": 7, "output": 12}
                )
            }
        )

        self.assertEqual(2, len(result))

    async def test_ok(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-tests.v1",
            spec=_functions["outcome-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        results = prepared_function.outcome.tests.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {"number": 10, "optional_number": 3333, "output": 11221122}
                )
            }
        )

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Ok", result.get("type"))


class TestFunctionOutcomeOkValue(unittest.IsolatedAsyncioTestCase):
    async def test_none_defined(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        self.assertIsNone(prepared_function.outcome.ok_value)

    async def test_value(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-ok-value.v1",
            spec=_functions["outcome-ok-value.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.outcome.ok_value.evaluate(
            {"inputs": celpy.json_to_cel({"output": 19283746})}
        )

        result: list = json.loads(json.dumps(cel_result))

        self.assertEqual(result, 19283746)

    async def test_no_output(self):
        prepared = await prepare.prepare_function(
            cache_key="outcome-ok-value.v1",
            spec=_functions["outcome-ok-value.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.outcome.ok_value.evaluate({})

        result: list = json.loads(json.dumps(cel_result))

        self.assertIsNone(result)


class TestFunctionMaterializerBase(unittest.IsolatedAsyncioTestCase):
    async def test_none_defined(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        self.assertIsNone(prepared_function.materializers.base)

    async def test_base(self):
        prepared = await prepare.prepare_function(
            cache_key="materializers-base-flat.v1",
            spec=_functions["materializers-base-flat.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.materializers.base.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {
                        "metadata": {"namespace": "my-test-namespace"},
                        "person": {"name": "Person Name", "age": 55},
                    }
                )
            }
        )

        result: list = json.loads(json.dumps(cel_result))

        print(result)
        self.maxDiff = None
        output = {
            "apiVersion": "tests.koreo.realkinetic.com/v1alpha8",
            "kind": "TestCase",
            "metadata.name": "base-test",
            "metadata.namespace": "my-test-namespace",
            "spec.name": "Person Name",
            "spec.age": 55,
            "spec.children.*.name": "Person Name, Jr.",
            "spec.children.*.age": 35,
        }
        self.assertDictEqual(output, result)

    async def test_nested_base(self):
        prepared = await prepare.prepare_function(
            cache_key="materializers-base-nested.v1",
            spec=_functions["materializers-base-nested.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.materializers.base.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {
                        "metadata": {"namespace": "my-test-namespace"},
                        "person": {"name": "Person Name", "age": 55},
                    }
                )
            }
        )

        result: list = json.loads(json.dumps(cel_result))

        self.maxDiff = None
        output = {
            "apiVersion": "tests.koreo.realkinetic.com/v1alpha8",
            "kind": "TestCase",
            "metadata.name": "base-test",
            "metadata.namespace": "my-test-namespace",
            "spec.name": "Person Name",
            "spec.age": 55,
            "spec['children.*'].name": "Person Name, Jr.",
            "spec['children.*'].age": 35,
        }
        self.assertDictEqual(output, result)


class TestFunctionMaterializerOnCreate(unittest.IsolatedAsyncioTestCase):
    async def test_none_defined(self):
        prepared = await prepare.prepare_function(
            cache_key="input-validation-tests.v1",
            spec=_functions["input-validation-tests.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        self.assertIsNone(prepared_function.materializers.on_create)

    async def test_flat(self):
        prepared = await prepare.prepare_function(
            cache_key="materializers-on-create-flat.v1",
            spec=_functions["materializers-on-create-flat.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.materializers.on_create.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {
                        "metadata": {"namespace": "my-test-namespace"},
                        "person": {"name": "Person Name", "age": 55},
                    }
                )
            }
        )

        result: list = json.loads(json.dumps(cel_result))

        print(result)
        self.maxDiff = None
        output = {
            "metadata.namespace": "my-test-namespace",
            "spec.name": "Person Name",
            "spec.age": 55,
            "spec.children.*": {
                "name": "Person Name, Jr.",
                "age": 35,
            },
        }
        self.assertDictEqual(output, result)

    async def test_nested(self):
        prepared = await prepare.prepare_function(
            cache_key="materializers-on-create-nested.v1",
            spec=_functions["materializers-on-create-nested.v1"].get("spec", {}),
        )

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_function, _ = prepared

        cel_result = prepared_function.materializers.on_create.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {
                        "metadata": {"namespace": "my-test-namespace"},
                        "person": {"name": "Person Name", "age": 55},
                    }
                )
            }
        )

        result: list = json.loads(json.dumps(cel_result))

        self.maxDiff = None
        output = {
            "metadata.namespace": "my-test-namespace",
            "spec.name": "Person Name",
            "spec.age": 55,
            "spec['children.*']": {
                "name": "Person Name, Jr.",
                "age": 35,
            },
        }
        self.assertDictEqual(output, result)
