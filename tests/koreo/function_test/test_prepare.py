import unittest

from koreo.result import DepSkip, is_error, is_unwrapped_ok

from koreo.function_test import prepare


class TestMalformedFunctionTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_spec(self):
        # None
        outcome = await prepare.prepare_function_test(cache_key="unit-test", spec=None)
        self.assertTrue(is_error(outcome))

        # Empty dict
        outcome = await prepare.prepare_function_test(cache_key="unit-test", spec={})
        self.assertTrue(is_error(outcome))

    async def test_missing_function(self):
        outcome = await prepare.prepare_function_test(
            cache_key="unit-test",
            spec={
                "functionRef": {
                    "kind": "ValueFunction",
                    "name": "unit-test-function.fake",
                },
                "testCases": [{"expectOutcome": {"ok": {}}}],
            },
        )
        assert is_unwrapped_ok(outcome), outcome.message

        function_test, _ = outcome
        self.assertIsInstance(function_test.function_under_test, DepSkip)
