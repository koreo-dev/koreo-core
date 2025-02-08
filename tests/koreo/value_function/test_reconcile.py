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
                preconditions=None,
                local_values=None,
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
                "preconditions": [
                    {
                        "assert": "=!inputs.preconditions.skip",
                        "skip": {"message": "=inputs.messages.skip"},
                    },
                    {
                        "assert": "=!inputs.preconditions.permFail",
                        "permFail": {"message": "=inputs.messages.permFail"},
                    },
                    {
                        "assert": "=!inputs.preconditions.depSkip",
                        "depSkip": {"message": "=inputs.messages.depSkip"},
                    },
                ],
                "locals": {
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
            "preconditions": {
                "skip": False,
                "permFail": False,
                "depSkip": False,
                "retry": False,
                "ok": False,
            },
            "messages": {
                "skip": "skip message",
                "permFail": "permFail message",
                "depSkip": "depSkip message",
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

    async def test_precondition_exits(self):
        predicate_pairs = (
            (
                "skip",
                result.Skip,
                {
                    "assert": "=!inputs.skip",
                    "skip": {"message": "skip message"},
                },
            ),
            (
                "permFail",
                result.PermFail,
                {
                    "assert": "=!inputs.permFail",
                    "permFail": {"message": "permFail message"},
                },
            ),
            (
                "depSkip",
                result.DepSkip,
                {
                    "assert": "=!inputs.depSkip",
                    "depSkip": {"message": "depSkip message"},
                },
            ),
            (
                "retry",
                result.Retry,
                {
                    "assert": "=!inputs.retry",
                    "retry": {"message": "retry message", "delay": 17},
                },
            ),
            (
                "ok",
                None,
                {"assert": "=!inputs.ok", "ok": {}},
            ),
        )

        predicates = [predicate for _, __, predicate in predicate_pairs]
        prepared_function = await prepare.prepare_value_function(
            cache_key="test", spec={"preconditions": predicates}
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

    async def test_return_with_locals(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "locals": {
                    "value": "=inputs.a + inputs.b",
                    "list": ["=inputs.a + inputs.b", 17, "constant"],
                    "map": {"mapKey": "=inputs.a + inputs.b"},
                },
                "return": {
                    "value": "=locals.value * locals.value",
                    "list": "=locals.list.map(value, string(value) + ' value')",
                    "map": "=locals.map.map(key, locals.map[key] * 3)",
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
            "value": 81,
            "list": ["9 value", "17 value", "constant value"],
            "map": [27],
        }

        self.maxDiff = None
        self.assertDictEqual(reconcile_result, expected_value)

    async def test_corrupt_precondition(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "preconditions": [
                    {
                        "assert": "='a' + 9",
                        "skip": {"message": "skip message"},
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
        self.assertIn("spec.preconditions", reconcile_result.message)

    async def test_corrupt_locals(self):
        prepared = await prepare.prepare_value_function(
            cache_key="test",
            spec={
                "locals": {
                    "busted": "='a' + 9",
                },
                "return": {
                    "value": "unused",  # Needed to prevent early-exit  eval of locals
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
        self.assertIn("spec.locals", reconcile_result.message)

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
        self.assertIn("spec.return", reconcile_result.message)
