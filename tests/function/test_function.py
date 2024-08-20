import unittest
import json

import celpy
import yaml

from koreo.function import prepare

_functions = {}

with open("crd/sample-function.yaml", "r") as raw_yamls:
    yamls = yaml.load_all(raw_yamls, Loader=yaml.Loader)
    for function_yaml in yamls:
        if function_yaml.get("kind") != "Function":
            continue
        metadata = function_yaml.get("metadata", {})
        key = f"{metadata.get('name')}"
        _functions[key] = function_yaml


class TestFunctionInputValidation(unittest.TestCase):
    def test_no_validators(self):
        prepared = prepare.prepare_function(_functions["empty"].get("spec", {}))
        self.assertIsNone(prepared.input_validators)

    def test_no_problems(self):
        prepared = prepare.prepare_function(
            _functions["input-validation-tests"].get("spec", {})
        )
        result = prepared.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 6, "optional_skip_number": 3333})}
        )

        self.assertFalse(result)

    def test_skip(self):
        prepared = prepare.prepare_function(
            _functions["input-validation-tests"].get("spec", {})
        )
        cel_result = prepared.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 4})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Skip", result.get("type"))
        self.assertIn("too small", result.get("message"))

    def test_optional_perm_fail(self):
        prepared = prepare.prepare_function(
            _functions["input-validation-tests"].get("spec", {})
        )
        cel_result = prepared.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 8, "optional_number": 250})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("PermFail", result.get("type"))
        self.assertIn("not equal", result.get("message"))

    def test_retry_delay(self):
        prepared = prepare.prepare_function(
            _functions["input-validation-tests"].get("spec", {})
        )
        cel_result = prepared.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 15})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Retry", result.get("type"))
        self.assertEqual(90, result.get("delay"))
        self.assertIn("too large", result.get("message"))

    def test_multiple_issues(self):
        prepared = prepare.prepare_function(
            _functions["input-validation-tests"].get("spec", {})
        )
        result = prepared.input_validators.evaluate(
            {"inputs": celpy.json_to_cel({"number": 0, "optional_retry_number": 7})}
        )

        self.assertEqual(3, len(result))


class TestFunctionOutcomeTests(unittest.TestCase):
    def test_none_defined(self):
        prepared = prepare.prepare_function(_functions["empty"].get("spec", {}))
        self.assertIsNone(prepared.outcome.tests)

    def test_optional_perm_fail(self):
        prepared = prepare.prepare_function(_functions["outcome-tests"].get("spec", {}))
        cel_result = prepared.outcome.tests.evaluate(
            {"inputs": celpy.json_to_cel({"number": 5, "optional_number": 250})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("PermFail", result.get("type"))
        self.assertIn("not equal", result.get("message"))

    def test_retry_delay(self):
        prepared = prepare.prepare_function(_functions["outcome-tests"].get("spec", {}))
        cel_result = prepared.outcome.tests.evaluate(
            {"inputs": celpy.json_to_cel({"number": 15})}
        )

        results: list = json.loads(json.dumps(cel_result))

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Retry", result.get("type"))
        self.assertEqual(90, result.get("delay"))
        self.assertIn("too large", result.get("message"))

    def test_multiple_issues(self):
        prepared = prepare.prepare_function(_functions["outcome-tests"].get("spec", {}))
        result = prepared.outcome.tests.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {"number": 20, "optional_number": 7, "output": 12}
                )
            }
        )

        self.assertEqual(2, len(result))

    def test_ok(self):
        prepared = prepare.prepare_function(_functions["outcome-tests"].get("spec", {}))
        results = prepared.outcome.tests.evaluate(
            {
                "inputs": celpy.json_to_cel(
                    {"number": 10, "optional_number": 3333, "output": 11221122}
                )
            }
        )

        self.assertEqual(1, len(results))

        result = results.pop()

        self.assertEqual("Ok", result.get("type"))


class TestFunctionOutcomeOkValue(unittest.TestCase):
    def test_none_defined(self):
        prepared = prepare.prepare_function(_functions["empty"].get("spec", {}))
        self.assertIsNone(prepared.outcome.ok_value)

    def test_value(self):
        prepared = prepare.prepare_function(
            _functions["outcome-ok-value"].get("spec", {})
        )
        cel_result = prepared.outcome.ok_value.evaluate(
            {"inputs": celpy.json_to_cel({"output": 19283746})}
        )

        result: list = json.loads(json.dumps(cel_result))

        self.assertEqual(result, 19283746)

    def test_no_output(self):
        prepared = prepare.prepare_function(
            _functions["outcome-ok-value"].get("spec", {})
        )
        cel_result = prepared.outcome.ok_value.evaluate({})

        result: list = json.loads(json.dumps(cel_result))

        self.assertIsNone(result)


class TestFunctionMaterializerBase(unittest.TestCase):
    def test_none_defined(self):
        prepared = prepare.prepare_function(_functions["empty"].get("spec", {}))
        self.assertIsNone(prepared.materializers.base)

    def test_base(self):
        prepared = prepare.prepare_function(
            _functions["materializers-base-flat"].get("spec", {})
        )
        cel_result = prepared.materializers.base.evaluate(
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

    def test_nested_base(self):
        prepared = prepare.prepare_function(
            _functions["materializers-base-nested"].get("spec", {})
        )
        cel_result = prepared.materializers.base.evaluate(
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
        print(result)
        self.assertDictEqual(output, result)


class TestFunctionMaterializerOnCreate(unittest.TestCase):
    def test_none_defined(self):
        prepared = prepare.prepare_function(_functions["empty"].get("spec", {}))
        self.assertIsNone(prepared.materializers.on_create)

    def test_flat(self):
        prepared = prepare.prepare_function(
            _functions["materializers-on-create-flat"].get("spec", {})
        )
        cel_result = prepared.materializers.on_create.evaluate(
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

    def test_nested(self):
        prepared = prepare.prepare_function(
            _functions["materializers-on-create-nested"].get("spec", {})
        )
        cel_result = prepared.materializers.on_create.evaluate(
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
        print(result)
        self.assertDictEqual(output, result)
