import unittest

from celpy import celtypes
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


class TestGroupRef(unittest.TestCase):
    def test_invalid_resource_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '"".group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_api_version(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"kind": "TestCase", "metadata": {"name": "a-name-value", "namespace": "some-namespace"}}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_kind(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "metadata": {"name": "a-name-value", "namespace": "some-namespace"}}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_metadata(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = (
            '{"apiVersion": "some.api.group/v2", "kind": "TestCase"}.group_ref()'
        )
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_name(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "metadata": {"namespace": "some-namespace"}}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_missing_namespace(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "metadata": {"name": "a-name-value"}}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_group_ref_from_api_version(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiVersion": "some.api.group/v2", "kind": "TestCase", "name": "a-name-value", "namespace": "some-namespace"}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual(
            {
                "apiGroup": "some.api.group",
                "kind": "TestCase",
                "name": "a-name-value",
                "namespace": "some-namespace",
            },
            result,
        )

    def test_group_ref_from_api_group(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '{"apiGroup": "some.api.group", "kind": "TestCase", "name": "a-name-value", "namespace": "some-namespace"}.group_ref()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)

        self.assertDictEqual(
            {
                "apiGroup": "some.api.group",
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

    def test_resource_is_not_mutated(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'a': inputs.base.overlay({'value': inputs.base.value + 1}), 'b': inputs.base.overlay({'value': inputs.base.value + 1})}"
        inputs = {"inputs": celpy.json_to_cel({"base": {"value": 1}})}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)
        self.maxDiff = None
        self.assertDictEqual(
            {"a": {"value": 2}, "b": {"value": 2}},
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

    def test_resource_with_deep_update(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "{'nested': {'deep': {'key': 'value'}}}.overlay({'nested': { 'deep': { 'new_key': 'new_value', 'deeper': {'nested': true}}}})"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        self.assertDictEqual(
            {
                "nested": {
                    "deep": {
                        "key": "value",
                        "new_key": "new_value",
                        "deeper": {"nested": True},
                    },
                }
            },
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

    def test_overlay_with_dots_in_name(self):
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
                },
                "metadata.deep.value.name": "a-name",
            },
            program.evaluate(inputs),
        )


class TestFlatten(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.flatten()"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_list(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "[].flatten()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertListEqual([], flattened)

    def test_flat(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "[1, 2, 3].flatten()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_single(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "[[1, 2, 3]].flatten()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertListEqual([1, 2, 3], flattened)

    def test_multiple(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = '[[1, 2, 3], ["a", "b", "c"]].flatten()'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertListEqual([1, 2, 3, "a", "b", "c"], flattened)


class TestSplit(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.split('.')"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_seperator(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split('')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertListEqual([""], flattened)

    def test_no_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.split('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertListEqual(result, ["test-ing-is-fun"])

    def test_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test.ing.is.fun'.split('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertListEqual(result, ["test", "ing", "is", "fun"])


class TestSplitFirst(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.split_first('.')"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_seperator(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_first('')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_first('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertEqual("", flattened)

    def test_no_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.split_first('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test-ing-is-fun", result)

    def test_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test.ing.is.fun'.split_first('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test", result)


class TestSplitIndex(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.split_index('.', 1)"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_seperator(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_index('', 1)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_out_of_bounds(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test'.split_index('.', 2)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_index('.', 1)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertEqual("", flattened)

    def test_no_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.split_index('.', 0)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test-ing-is-fun", result)

    def test_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test.ing.is.fun'.split_index('.', 2)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("is", result)


class TestSplitLast(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.split_last('.')"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_seperator(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_last('')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.split_last('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertEqual("", flattened)

    def test_no_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.split_last('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test-ing-is-fun", result)

    def test_spilt(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test.ing.is.fun'.split_last('.')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("fun", result)


class TestStrip(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.strip('test')"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_on(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "test.strip('')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.strip('test')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertEqual("", flattened)

    def test_no_strip(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.strip('b')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test-ing-is-fun", result)

    def test_strip(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'testinga'.strip('testing')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("a", result)


class TestRStrip(unittest.TestCase):
    def test_invalid_type(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "1.rstrip('test')"

        with self.assertRaises(celpy.CELParseError):
            cel_env.compile(test_cel_expression)

    def test_empty_on(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "test.rstrip('')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_empty_target(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "''.rstrip('test')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        flattened = program.evaluate(inputs)
        self.assertEqual("", flattened)

    def test_no_rstrip(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'test-ing-is-fun'.rstrip('b')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("test-ing-is-fun", result)

    def test_rstrip(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "'testingisfun'.rstrip('isfun')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("testing", result)


class TestToJson(unittest.TestCase):
    def test_to_json_null(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "to_json()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_to_json_str(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "to_json('isfun')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual('"isfun"', result)

    def test_to_json_map(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'to_json({"testKey": "testValue"})'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual('{"testKey": "testValue"}', result)

    def test_to_json_list(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'to_json(["testKey", "testValue"])'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual('["testKey", "testValue"]', result)

    def test_to_json_bool(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "to_json(false)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("0", result)

    def test_to_json_int(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "to_json(3)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("3", result)


class TestFromJson(unittest.TestCase):
    def test_from_json_null(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "from_json()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_from_json_str(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "from_json('isfun')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_from_json_map(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'from_json(\'{"testKey": "testValue"}\')'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual({"testKey": "testValue"}, result)

    def test_from_json_list(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'from_json(\'["testKey", "testValue"]\')'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual(["testKey", "testValue"], result)

    def test_from_json_bool(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "from_json(false)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_from_json_int(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "from_json(3)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)


class TestBase64Encode(unittest.TestCase):
    def test_b64encode_null(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64encode()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_b64encode_str(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64encode('isfun')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("aXNmdW4=", result)

    def test_b64encode_map(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'b64encode({"testKey": "testValue"})'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_b64encode_list(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = 'b64encode(["testKey", "testValue"])'
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_b64encode_bool(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64encode(false)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_b64encode_int(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64encode(3)"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)


class TestBase64Decode(unittest.TestCase):
    def test_b64decode_null(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64decode()"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)

    def test_b64decode_str(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64decode('aXNmdW4=')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        result = program.evaluate(inputs)
        self.assertEqual("isfun", result)

    def test_b64encode_invalid(self):
        cel_env = celpy.Environment(annotations=koreo_function_annotations)

        test_cel_expression = "b64decode('aXNmdW')"
        inputs = {}

        compiled = cel_env.compile(test_cel_expression)
        program = cel_env.program(compiled, functions=koreo_cel_functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(inputs)
