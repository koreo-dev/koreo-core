import unittest

import celpy

from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations


class TestToRef(unittest.TestCase):
    def test_invalid_type_to_ref(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '"".to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_name_and_external(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            '{"apiVersion": "some.api.group/v1", "kind": "TestCase"}.to_ref()'
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_to_ref_empty_name(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"name": ""}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_to_ref_name_only(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"name": "test-case"}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual({"name": "test-case"}, result)

    def test_to_ref_name_full(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "name": "test-case", "namespace": "a-namespace", "extra": "value"}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual(
            {
                "apiVersion": "some.api.group/v2",
                "kind": "TestCase",
                "name": "test-case",
                "namespace": "a-namespace",
            },
            result,
        )

    def test_to_ref_external_only(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"external": "thing/some_id"}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual({"external": "thing/some_id"}, result)

    def test_to_ref_empty_external(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"external": ""}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_to_ref_external_full(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "external": "group/id"}.to_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual(
            {
                "apiVersion": "some.api.group/v2",
                "kind": "TestCase",
                "external": "group/id",
            },
            result,
        )


class TestSelfRef(unittest.TestCase):
    def test_invalid_resource_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '"".self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_api_version(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"kind": "TestCase", "metadata": {"name": "a-name-value", "namespace": "some-namespace"}}.self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_kind(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "metadata": {"name": "a-name-value", "namespace": "some-namespace"}}.self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_metadata(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            '{"apiVersion": "some.api.group/v2", "kind": "TestCase"}.self_ref()'
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_name(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "metadata": {"namespace": "some-namespace"}}.self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_namespace(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "metadata": {"name": "a-name-value"}}.self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_to_ref_external_full(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "metadata": {"name": "a-name-value", "namespace": "some-namespace"}}.self_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual(
            {
                "apiVersion": "some.api.group/v2",
                "kind": "TestCase",
                "name": "a-name-value",
                "namespace": "some-namespace",
            },
            result,
        )


class TestConfigConnectReady(unittest.TestCase):
    def test_invalid_resource_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '"".config_connect_ready()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_no_status(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_no_conditions(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_no_ready_condition(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {'conditions': [{}]}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_two_ready_conditions(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {'conditions': [{'type': 'Ready'}, {'type': 'Ready'}]}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_ready_condition_non_reason(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {'conditions': [{'type': 'Ready', 'reason': 'Testing'}]}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_non_ready_condition_non_status(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {'conditions': [{'type': 'Ready', 'reason': 'UpToDate', 'status': 'False'}]}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertFalse(result)

    def test_ready_condition_true(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'status': {'conditions': [{'type': 'Ready', 'reason': 'UpToDate', 'status': 'True'}]}}.config_connect_ready()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertTrue(result)


class TestTemplateName(unittest.TestCase):
    def test_invalid_type_to_ref(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '"".template_name("bad")'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_api_version(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"kind": "TestCase"}.template_name("bad")'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_kind(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v1"}.template_name("bad")'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_bad_name(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            '{"apiVersion": "api.group/v5", "kind": "TestCase"}.template_name("")'
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_ok(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "api.group/v5", "kind": "TestCase"}.template_name("template")'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertEqual("testcase.api.group.v5.template", program.evaluate(inputs))


class TestOverlay(unittest.TestCase):
    def test_empty_resource_and_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{}.overlay({})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual({}, program.evaluate(inputs))

    def test_resource_and_empty_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            "{'a': 'key', 'value': 18, 'bool': true, 'cel': 1 + 32}.overlay({})"
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {"a": "key", "value": 18, "bool": True, "cel": 33},
            program.evaluate(inputs),
        )

    def test_empty_resource_with_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            "{}.overlay({'a': 'key', 'value': 18, 'bool': true, 'cel': 1 + 32})"
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {"a": "key", "value": 18, "bool": True, "cel": 33},
            program.evaluate(inputs),
        )

    def test_resource_with_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'a': 'wrong', 'value': 99, 'bool': false}.overlay({'a': 'key', 'value': 18, 'bool': true, 'cel': 1 + 32})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {"a": "key", "value": 18, "bool": True, "cel": 33},
            program.evaluate(inputs),
        )

    def test_resource_with_deep_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'nested': {'key': 'value'}}.overlay({'nested': {'key': 'value'}, 'new': {'nested': 'a' + string(1 + 8)}})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {"nested": {"key": "value"}, "new": {"nested": "a9"}},
            program.evaluate(inputs),
        )

    def test_resource_with_labels_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'metadata': {'labels': {'some.group/key': 'value'}, 'annotations': {'a.group/key': 'value'}}}.overlay({'metadata': {'labels': {'some.group/key': string(9 + 10 + 88)}, 'annotations': {'a.group/key': 'a ' + 'b ' + 'c'}}})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {
                "metadata": {
                    "labels": {"some.group/key": "107"},
                    "annotations": {"a.group/key": "a b c"},
                }
            },
            program.evaluate(inputs),
        )

    def test_resource_with_json_path_overlay(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'metadata': {'labels': {'some.group/key': 'value'}, 'annotations': {'a.group/key': 'value'}}}.overlay({'metadata.deep.value.name': 'a' + '-' + 'name'})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {
                "metadata": {
                    "labels": {"some.group/key": "value"},
                    "annotations": {"a.group/key": "value"},
                    "deep": {"value": {"name": "a-name"}},
                }
            },
            program.evaluate(inputs),
        )
