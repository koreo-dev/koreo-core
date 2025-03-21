from random import shuffle
import unittest

from koreo import result


class TestCombineMessages(unittest.TestCase):
    def test_no_outcomes(self):
        outcomes: list[result.Outcome] = []

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Skip)

    def test_depskips(self):
        outcomes: list[result.Outcome] = [
            result.DepSkip(),
            result.DepSkip(),
            result.DepSkip(),
            result.DepSkip(),
            result.DepSkip(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.DepSkip)

    def test_skips(self):
        outcomes: list[result.Outcome] = [
            result.Skip(),
            result.Skip(),
            result.Skip(),
            result.Skip(),
            result.Skip(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Skip)

    def test_depskips_and_skips(self):
        outcomes: list[result.Outcome] = [
            result.DepSkip(),
            result.Skip(),
            result.DepSkip(),
            result.Skip(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Skip)

    def test_oks(self):
        outcomes: list[result.Outcome] = [
            result.Ok("test"),
            result.Ok(8),
            result.Ok(88),
            result.Ok(None),
            result.Ok(True),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Ok)

        # Note, this is done so that we can shuffle the outcomes so that we're
        # testing different combinations of outcome orderings.
        for value in [None, "test", 8, 88, True]:
            self.assertIn(value, combined.data)

    def test_skips_and_oks(self):
        outcomes: list[result.Outcome] = [
            result.DepSkip(),
            result.DepSkip(),
            result.Ok("test"),
            result.Ok(8),
            result.Ok(False),
            result.Ok(None),
            result.Skip(),
            result.Skip(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Ok)

        # Note, this is done so that we can shuffle the outcomes so that we're
        # testing different combinations of outcome orderings.
        for value in [None, "test", 8, False]:
            self.assertIn(value, combined.data)

    def test_combine_one_value(self):
        outcomes: list[result.Outcome] = [
            result.Ok("ok-value"),
        ]

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Ok)

        print(combined)
        self.assertListEqual(["ok-value"], combined.data)

    def test_unwrapped_combine_one_value(self):
        outcomes: list[result.UnwrappedOutcome] = [
            "one-ok-value",
        ]

        combined = result.unwrapped_combine(outcomes)

        print(combined)
        self.assertListEqual(["one-ok-value"], combined)

    def test_retries(self):
        delay_5_message = "Waiting"
        default_delay_message = "Will retry"

        outcomes: list[result.Outcome] = [
            result.Retry(delay=500),
            result.Retry(delay=5, message="Waiting"),
            result.Retry(delay=59),
            result.Retry(message="Will retry"),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Retry)
        self.assertEqual(500, combined.delay)
        self.assertIn(delay_5_message, combined.message)
        self.assertIn(default_delay_message, combined.message)

    def test_skips_oks_and_retries(self):
        outcomes: list[result.Outcome] = [
            result.DepSkip(),
            result.Skip(),
            result.Ok(None),
            result.Ok("test"),
            result.Ok(8),
            result.Retry(delay=500),
            result.Retry(delay=5, message="Waiting"),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Retry)
        self.assertEqual(500, combined.delay)
        self.assertEqual("Waiting", combined.message)

    def test_permfail(self):
        first_message = "A bad error"
        second_message = "A really, really bad error"

        outcomes: list[result.Outcome] = [
            result.PermFail(),
            result.PermFail(message=first_message),
            result.PermFail(message=second_message),
            result.PermFail(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.PermFail)
        self.assertIn(first_message, combined.message)
        self.assertIn(second_message, combined.message)

    def test_skips_oks_retries_and_permfail(self):
        outcomes: list[result.Outcome] = [
            result.DepSkip(),
            result.Skip(),
            result.Ok(None),
            result.Ok("test"),
            result.Ok(8),
            result.Retry(delay=500),
            result.Retry(delay=5, message="Waiting"),
            result.PermFail(),
            result.PermFail(message="All done"),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.PermFail)
        self.assertEqual("All done", combined.message)


class TestIsOk(unittest.TestCase):
    def test_skip(self):
        self.assertFalse(result.is_ok(result.Skip()))

    def test_ok(self):
        self.assertTrue(result.is_ok(result.Ok(None)))

    def test_retry(self):
        self.assertFalse(result.is_ok(result.Retry()))

    def test_permfail(self):
        self.assertFalse(result.is_ok(result.PermFail()))


class TestIsNotError(unittest.TestCase):
    def test_skip(self):
        self.assertTrue(result.is_not_error(result.Skip()))

    def test_ok(self):
        self.assertTrue(result.is_not_error(result.Ok(None)))

    def test_retry(self):
        self.assertFalse(result.is_not_error(result.Retry()))

    def test_permfail(self):
        self.assertFalse(result.is_not_error(result.PermFail()))


class TestIsError(unittest.TestCase):
    def test_skip(self):
        self.assertFalse(result.is_error(result.Skip()))

    def test_ok(self):
        self.assertFalse(result.is_error(result.Ok(None)))

    def test_retry(self):
        self.assertTrue(result.is_error(result.Retry()))

    def test_permfail(self):
        self.assertTrue(result.is_error(result.PermFail()))
