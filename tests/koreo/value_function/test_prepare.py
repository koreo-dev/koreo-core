import unittest

import celpy
from celpy import celtypes

from koreo import result
from koreo.cel.evaluation import evaluate_overlay
from koreo.value_function import prepare
from koreo.value_function.structure import ValueFunction


class TestValueFunctionPrepare(unittest.IsolatedAsyncioTestCase):
    async def test_full_spec(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "preconditions": [
                    {"assert": "=!inputs.skip", "skip": {"message": "Skip"}},
                    {
                        "assert": "=!inputs.permFail",
                        "permFail": {"message": "Perm Fail"},
                    },
                    {
                        "assert": "=!inputs.depSkip",
                        "depSkip": {"message": "Dep Skip"},
                    },
                ],
                "locals": {
                    "some_list": [1, 2, 3, 4],
                    "a_value_map": {"a": "b"},
                    "integer": 7,
                    "a_none": None,
                    "cel_expr": "=1 + 17 / 2",
                },
                "return": {
                    "string": "1 + 1",
                    "simple_cel": "=1 + 1",
                    "nested": {
                        "string": "this is a test",
                        "simple_cel": "='a fun' + ' test'",
                    },
                    "list": [
                        1,
                        2,
                        3,
                        "hopefully",
                        "it",
                        "works",
                        "=1 - 2",
                        "='a string' + ' concat'",
                    ],
                },
            },
        )

        function, _ = prepared
        self.assertIsInstance(
            function,
            ValueFunction,
        )

    async def test_missing_and_bad_spec(self):
        bad_specs = [None, {}, "asda", [], 2, True]
        for bad_spec in bad_specs:
            outcome = await prepare.prepare_value_function(
                cache_key="test", spec=bad_spec
            )
            self.assertIsInstance(
                outcome,
                result.PermFail,
                msg=f'Expected `PermFail` for malformed `spec` "{bad_spec}"',
            )

    async def test_malformed_locals(self):
        bad_locals = ["asda", [], 2, True]
        for bad_locals in bad_locals:
            outcome = await prepare.prepare_value_function(
                cache_key="test", spec={"locals": bad_locals}
            )
            self.assertIsInstance(
                outcome,
                result.PermFail,
                msg=f'Expected `PermFail` for malformed `spec.locals` "{bad_locals}"',
            )

    async def test_preconditions_none_and_empty_list(self):
        outcome = await prepare.prepare_value_function("test", {"preconditions": None})

        self.assertIsInstance(
            outcome,
            result.PermFail,
        )

        prepared = await prepare.prepare_value_function("test", {"preconditions": []})
        function, _ = prepared
        self.assertIsInstance(
            function,
            ValueFunction,
            msg="Unexpected error with empty list `spec.preconditions`",
        )

    async def test_bad_precondition_input_type(self):
        bad_values = [1, "abc", {"value": "one"}, True]
        for value in bad_values:
            self.assertIsInstance(
                await prepare.prepare_value_function("test", {"preconditions": value}),
                result.PermFail,
                msg=f"Expected PermFail for bad `predicate_spec` '{value}' (type: {type(value)})",
            )

    async def test_malformed_precondition_input(self):
        bad_values = [
            {"skip": {"message": "=1 + missing"}},
            {"assert": "=1 / 0 '", "permFail": {"message": "Bogus assert"}},
        ]
        self.assertIsInstance(
            await prepare.prepare_value_function("test", {"preconditions": bad_values}),
            result.PermFail,
        )

    async def test_none_and_empty_list_return(self):
        outcome = await prepare.prepare_value_function("test", {"return": None})
        self.assertIsInstance(outcome, result.PermFail)

        function, _ = await prepare.prepare_value_function("test", {"return": {}})
        self.assertIsInstance(
            function,
            ValueFunction,
            msg="Unexpected error with empty map `return`",
        )

    async def test_bad_return_input_type(self):
        bad_values = [1, "abc", ["value", "one"], True]
        for value in bad_values:
            self.assertIsInstance(
                await prepare.prepare_value_function("test", {"return": value}),
                result.PermFail,
                msg=f"Expected PermFail for bad `return` '{value}' (type: {type(value)})",
            )

    async def test_malformed_return_input(self):
        bad_values = {
            "skip": {"message": "=1 + missing"},
            "assert": "=1 / 0 '",
            "permFail": {"message": "Bogus assert"},
        }
        self.assertIsInstance(
            await prepare.prepare_value_function("test", {"return": bad_values}),
            result.PermFail,
        )

    async def test_ok_return_input(self):

        return_value_cel = {
            "string": "1 + 1",
            "simple_cel": "=1 + 1",
            "nested": {
                "string": "this is a test",
                "simple_cel": "='a fun' + ' test'",
            },
            "list": [
                1,
                2,
                3,
                "hopefully",
                "it",
                "works",
                "=1 - 2",
                "='a string' + ' concat'",
            ],
        }

        inputs = {
            "skip": celtypes.BoolType(False),
            "permFail": celtypes.BoolType(False),
            "depSkip": celtypes.BoolType(False),
            "retry": celtypes.BoolType(False),
            "ok": celtypes.BoolType(False),
        }

        expected_return = {
            "string": "1 + 1",
            "simple_cel": 2,
            "nested": {
                "string": "this is a test",
                "simple_cel": "a fun test",
            },
            "list": [
                1,
                2,
                3,
                "hopefully",
                "it",
                "works",
                -1,
                "a string concat",
            ],
        }

        prepared = await prepare.prepare_value_function(
            "test", {"return": return_value_cel}
        )

        assert result.is_unwrapped_ok(prepared)

        function, _ = prepared

        assert function.return_value is not None

        return_value = evaluate_overlay(
            overlay=function.return_value,
            inputs=inputs,
            base=celtypes.MapType({}),
            location="unittest",
        )
        self.assertDictEqual(
            return_value,
            expected_return,
        )
