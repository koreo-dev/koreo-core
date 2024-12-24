import unittest

import celpy

from koreo import result
from koreo.value_function import prepare
from koreo.value_function.structure import ValueFunction


class TestValueFunctionPrepare(unittest.IsolatedAsyncioTestCase):
    async def test_full_spec(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "validators": [
                    {"assert": "=inputs.skip", "skip": {"message": "SKip"}},
                    {
                        "assert": "=inputs.permFail",
                        "permFail": {"message": "Perm Fail"},
                    },
                    {"assert": "=inputs.depSkip", "depSkip": {"message": "Dep Skip"}},
                ],
                "constants": {
                    "some_list": [1, 2, 3, 4],
                    "a_value_map": {"a": "b"},
                    "integer": 7,
                    "a_none": None,
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

    async def test_malformed_constants(self):
        bad_constants = ["asda", [], 2, True]
        for bad_constants in bad_constants:
            outcome = await prepare.prepare_value_function(
                cache_key="test", spec={"constants": bad_constants}
            )
            self.assertIsInstance(
                outcome,
                result.PermFail,
                msg=f'Expected `PermFail` for malformed `spec.constants` "{bad_constants}"',
            )

    async def test_validators_none_and_empty_list(self):
        function, _ = await prepare.prepare_value_function("test", {"validators": None})

        self.assertIsInstance(
            function,
            ValueFunction,
            msg="Unexpected error with `None` `spec.validators`",
        )

        function, _ = await prepare.prepare_value_function("test", {"validators": []})
        self.assertIsInstance(
            function,
            ValueFunction,
            msg="Unexpected error with empty list `spec.validators`",
        )

    async def test_bad_validator_input_type(self):
        bad_values = [1, "abc", {"value": "one"}, True]
        for value in bad_values:
            self.assertIsInstance(
                await prepare.prepare_value_function("test", {"validators": value}),
                result.PermFail,
                msg=f"Expected PermFail for bad `predicate_spec` '{value}' (type: {type(value)})",
            )

    async def test_malformed_validator_input(self):
        bad_values = [
            {"skip": {"message": "=1 + missing"}},
            {"assert": "=1 / 0 '", "permFail": {"message": "Bogus assert"}},
        ]
        self.assertIsInstance(
            await prepare.prepare_value_function("test", {"validators": bad_values}),
            result.PermFail,
        )

    async def test_none_and_empty_list_return(self):
        function, _ = await prepare.prepare_value_function("test", {"return": None})
        self.assertIsInstance(
            function,
            ValueFunction,
            msg="Unexpected error with missing `return`",
        )

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

        inputs = celpy.json_to_cel(
            {
                "skip": False,
                "permFail": False,
                "depSkip": False,
                "retry": False,
                "ok": False,
            }
        )

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

        function, _ = await prepare.prepare_value_function(
            "test", {"return": return_value_cel}
        )
        self.assertDictEqual(
            function.return_value.evaluate({"inputs": celpy.json_to_cel(inputs)}),
            expected_return,
        )
