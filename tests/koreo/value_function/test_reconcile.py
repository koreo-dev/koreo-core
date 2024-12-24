import copy
import unittest

import celpy

from koreo import result

from koreo.value_function import prepare
from koreo.value_function import reconcile
from koreo.value_function.structure import ValueFunction


class TestReconcileValueFunction(unittest.IsolatedAsyncioTestCase):

    async def test_no_spec_returns_none(self):
        result = await reconcile.reconcile_value_function(
            location="test-fn",
            function=ValueFunction(
                validators=None,
                constants=celpy.json_to_cel({}),
                return_value=None,
                dynamic_input_keys=set(),
            ),
            inputs=celpy.json_to_cel({}),
        )

        self.assertEqual(result, None)

    async def test_full_scenario(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "validators": [
                    {
                        "assert": "=inputs.validators.skip",
                        "skip": {"message": "=constants.messages.skip"},
                    },
                    {
                        "assert": "=inputs.validators.permFail",
                        "permFail": {"message": "=constants.messages.permFail"},
                    },
                    {
                        "assert": "=inputs.validators.depSkip",
                        "depSkip": {"message": "=constants.messages.depSkip"},
                    },
                ],
                "constants": {
                    "messages": {
                        "skip": "skip message",
                        "permFail": "permFail message",
                        "depSkip": "depSkip message",
                    },
                    "mapKey": "a-key",
                },
                "return": {
                    "simple_cel": "=inputs.ints.a + inputs.ints.b",
                    "list": ["=inputs.ints.a + inputs.ints.b", 17, "constant"],
                    "map": {"mapKey": "=inputs.ints.a + inputs.ints.b"},
                },
            },
        )

        function, _ = prepared

        base_inputs = {
            "validators": {
                "skip": False,
                "permFail": False,
                "depSkip": False,
                "retry": False,
                "ok": False,
            },
            "ints": {"a": 1, "b": 8},
        }

        reconcile_result = await reconcile.reconcile_value_function(
            location="test-fn",
            function=function,
            inputs=celpy.json_to_cel(base_inputs),
        )

        expected_value = {
            "simple_cel": 9,
            "list": [9, 17, "constant"],
            "map": {"mapKey": 9},
        }

        self.assertDictEqual(reconcile_result, expected_value)

    async def test_validator_exits(self):
        predicate_pairs = (
            (
                "skip",
                result.Skip,
                {
                    "assert": "=inputs.skip",
                    "skip": {"message": "=constants.messages.skip"},
                },
            ),
            (
                "permFail",
                result.PermFail,
                {
                    "assert": "=inputs.permFail",
                    "permFail": {"message": "=constants.messages.permFail"},
                },
            ),
            (
                "depSkip",
                result.DepSkip,
                {
                    "assert": "=inputs.depSkip",
                    "depSkip": {"message": "=constants.messages.depSkip"},
                },
            ),
            (
                "retry",
                result.Retry,
                {
                    "assert": "=inputs.retry",
                    "retry": {"message": "=constants.messages.retry", "delay": 17},
                },
            ),
            (
                "ok",
                None,
                {"assert": "=inputs.ok", "ok": {}},
            ),
            (
                "bogus",
                result.PermFail,
                {
                    "assert": "=inputs.bogus",
                    "bogus": {"message": "=constants.messages.bogus", "whatever": True},
                },
            ),
        )

        predicates = [predicate for _, __, predicate in predicate_pairs]
        prepared_function = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "validators": predicates,
                "constants": {
                    "messages": {
                        "skip": "skip message",
                        "depSkip": "depSkip message",
                        "permFail": "permFail message",
                        "retry": "retry message",
                        "bogus": "bogus message",
                    },
                },
            },
        )
        assert isinstance(prepared_function, tuple)
        function, _ = prepared_function

        base_inputs = {
            "skip": False,
            "permFail": False,
            "depSkip": False,
            "retry": False,
            "ok": False,
            "bogus": False,
        }
        for input_key, expected_type, _ in predicate_pairs:
            test_inputs = copy.deepcopy(base_inputs)
            test_inputs[input_key] = True

            reconcile_result = await reconcile.reconcile_value_function(
                location="test-fn",
                function=function,
                inputs=celpy.json_to_cel(test_inputs),
            )

            print(f"test case: {input_key}")
            print(reconcile_result)
            print(expected_type)
            if expected_type is None:
                self.assertIsNone(reconcile_result)
            else:
                self.assertIsInstance(reconcile_result, expected_type)
                if input_key == "bogus":
                    self.assertTrue(
                        reconcile_result.message.startswith("Unknown predicate type")
                    )
                else:
                    self.assertEqual(reconcile_result.message, f"{input_key} message")

    async def test_simple_return(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "return": {
                    "value": "=inputs.a + inputs.b",
                    "list": ["=inputs.a + inputs.b", 17, "constant"],
                    "map": {"mapKey": "=inputs.a + inputs.b"},
                },
            },
        )

        function, _ = prepared

        base_inputs = {"a": 1, "b": 8}

        reconcile_result = await reconcile.reconcile_value_function(
            location="test-fn",
            function=function,
            inputs=celpy.json_to_cel(base_inputs),
        )

        expected_value = {
            "value": 9,
            "list": [9, 17, "constant"],
            "map": {"mapKey": 9},
        }

        self.assertDictEqual(reconcile_result, expected_value)

    async def test_corrupt_validator(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "validators": [
                    {
                        "assert": "='a' + 9",
                        "skip": {"message": "=constants.messages.skip"},
                    },
                ],
            },
        )

        function, _ = prepared

        base_inputs = {}

        reconcile_result = await reconcile.reconcile_value_function(
            location="test-fn",
            function=function,
            inputs=celpy.json_to_cel(base_inputs),
        )

        self.assertIsInstance(reconcile_result, result.PermFail)
        self.assertIn("evaluating `validators`", reconcile_result.message)

    async def test_corrupt_return(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "return": {
                    "value": "='a' + 18",
                },
            },
        )

        function, _ = prepared

        base_inputs = {}

        reconcile_result = await reconcile.reconcile_value_function(
            location="test-fn",
            function=function,
            inputs=celpy.json_to_cel(base_inputs),
        )

        self.assertIsInstance(reconcile_result, result.PermFail)
        self.assertIn("evaluating `return value`", reconcile_result.message)
