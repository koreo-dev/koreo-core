import copy
import unittest

from koreo.resource_function import reconcile


class TestDifferenceValidator(unittest.TestCase):
    def test_basics(self):

        cases = [
            ("", ""),
            (None, None),
            (True, True),
            (False, False),
            (0, 0),
            (1, 1),
            (10.2, 10.2),
            ([], []),
            ((), ()),
            ({}, {}),
        ]

        for target, actual in cases:
            match = reconcile._validate_match(target=target, actual=actual)
            self.assertFalse(match.differences)
            self.assertTrue(match.match)

    def test_type_mismatches(self):
        types = ["", None, False, True, [], {}, 0, 1, 8.2]

        cases = [
            (base, other)
            for base_idx, base in enumerate(types)
            for other_idx, other in enumerate(types)
            if base_idx != other_idx
        ]

        for target, actual in cases:
            match = reconcile._validate_match(target=target, actual=actual)
            self.assertTrue(
                match.differences,
                f"Failed to detect type-mismatch between '{target}' and '{actual}'",
            )
            self.assertFalse(match.match)

    def test_dict_match(self):
        target_list = {
            "one": 1,
            "two": False,
            "three": {
                "three.1": 0,
                "three.2": 1,
                "three.3": True,
                "three.4": False,
                "three.5": [1, 2, "a", "b", True, False, {"value": 1}],
                "three.6": {
                    "one": 1,
                    "a": "b",
                    "c": True,
                    "d": False,
                    "e": {"value": 1},
                },
            },
            "four": 7.2,
        }
        actual_list = copy.deepcopy(target_list)

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_dict_mismatch(self):
        target_list = {
            "one": 1,
            "two": False,
            "three": {
                "three.1": 0,
                "three.2": 1,
                "three.3": True,
                "three.4": False,
                "three.5": [1, 2, "a", "b", True, False, {"value": 1}],
                "three.6": {
                    "one": 1,
                    "a": "b",
                    "c": True,
                    "d": False,
                    "e": {"value": 1},
                },
            },
            "four": 7.2,
        }
        actual_list = copy.deepcopy(target_list)
        actual_list["three"]["three.5"][6]["value"] = 2

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertIn("index '6'", "".join(match.differences))
        self.assertFalse(match.match)

    def test_dict_missing_key(self):
        target_list = {
            "one": 1,
            "two": False,
            "three": {"value": 1},
            "four": 7.2,
        }
        actual_list = copy.deepcopy(target_list)
        del actual_list["three"]

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertIn("missing", "".join(match.differences))
        self.assertFalse(match.match)

    def test_dict_owner_refs_skipped(self):
        target_list = {
            "one": 1,
            "two": False,
        }
        actual_list = copy.deepcopy(target_list)

        target_list["ownerReferences"] = {"one": "value"}
        actual_list["ownerReferences"] = {"one": "dfferent value"}

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_list_match(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = target_list.copy()

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_list_length_mismatch(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = [1, "two", {"three": 4, "five": 6}]

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertIn("length", "".join(match.differences))
        self.assertFalse(match.match)

    def test_list_mismatch(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = [1, "two", {"three": 4, "five": 6}, 7.2]

        match = reconcile._validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertFalse(match.match)
