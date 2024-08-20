from random import shuffle
import unittest

import kopf

from koreo import result


class TestCombineMessages(unittest.TestCase):
    def test_no_outcomes(self):
        outcomes: result.Outcomes = []

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Skip)

    def test_depskips(self):
        outcomes: result.Outcomes = [
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
        outcomes: result.Outcomes = [
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
        outcomes: result.Outcomes = [
            result.DepSkip(),
            result.Skip(),
            result.DepSkip(),
            result.Skip(),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Skip)

    def test_oks(self):
        outcomes: result.Outcomes = [
            result.Ok(None),
            result.Ok("test"),
            result.Ok(8),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Ok)

        self.assertIn("test", combined.data)
        self.assertIn(8, combined.data)
        self.assertNotIn(None, combined.data)

    def test_skips_and_oks(self):
        outcomes: result.Outcomes = [
            result.DepSkip(),
            result.Skip(),
            result.Ok(None),
            result.Ok("test"),
            result.Ok(8),
        ]
        shuffle(outcomes)

        combined = result.combine(outcomes)

        self.assertIsInstance(combined, result.Ok)

        self.assertIn("test", combined.data)
        self.assertIn(8, combined.data)
        self.assertNotIn(None, combined.data)

    def test_retries(self):
        delay_5_message = "Waiting"
        default_delay_message = "Will retry"

        outcomes: result.Outcomes = [
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
        outcomes: result.Outcomes = [
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

        outcomes: result.Outcomes = [
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
        outcomes: result.Outcomes = [
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


class TestRaiseForError(unittest.TestCase):
    def test_retry(self):
        with self.assertRaisesRegex(kopf.TemporaryError, "Try again"):
            result.raise_for_error(result.Retry(message="Try again"))

    def test_permfail(self):
        with self.assertRaisesRegex(kopf.PermanentError, "Failure"):
            result.raise_for_error(result.PermFail(message="Failure"))
