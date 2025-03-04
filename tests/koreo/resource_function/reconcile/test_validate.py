import copy
import random
import string
import unittest

from celpy import celtypes

from koreo.resource_function.reconcile import validate

CHARS = string.ascii_letters + " "


def string_generator(size: int):
    return "".join(random.choices(population=CHARS, k=size))


class TestListValidator(unittest.TestCase):
    def test_match(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:]
            rhs = objects[:]

            match = validate._validate_list_match(target=lhs, actual=rhs)
            self.assertFalse(match.differences, f"{label} mismatch")
            self.assertTrue(match.match, f"{label} mismatch")

    def test_matching_lists_out_of_order(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:]

            random.shuffle(objects)

            rhs = objects[:]

            match = validate._validate_list_match(target=lhs, actual=rhs)
            self.assertTrue(match.differences, f"{label} mismatch")
            self.assertFalse(match.match, f"{label} mismatch")

    def test_general_mismatch(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            lhs = [generator() for _ in range(random.randint(0, 15))]

            rhs = [generator() for _ in range(random.randint(0, 15))]

            match = validate._validate_list_match(target=lhs, actual=rhs)
            self.assertTrue(match.differences, f"{label} mismatch")
            self.assertFalse(match.match, f"{label} mismatch")

    def test_extra_values(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:-3]
            rhs = objects[:]

            match = validate._validate_list_match(target=lhs, actual=rhs)
            self.assertEqual(1, len(match.differences), f"{label} mismatch")
            self.assertFalse(match.match, f"{label} mismatch")

            (difference_message, *_) = match.differences
            self.assertIn("length", difference_message, f"{label} mismatch")

    def test_nulls(self):
        non_nulls = [
            string_generator(random.randint(5, 50))
            for _ in range(random.randint(5, 50))
        ]

        match = validate._validate_list_match(target=non_nulls, actual=None)
        self.assertFalse(match.match)
        (difference_message, *_) = match.differences
        self.assertIn("null array", difference_message)

        match = validate._validate_list_match(target=None, actual=non_nulls)
        self.assertFalse(match.match)
        (difference_message, *_) = match.differences
        self.assertIn("null array", difference_message)

        match = validate._validate_list_match(target=None, actual=None)
        self.assertTrue(match.match)
        self.assertFalse(match.differences)

    def test_last_applied_matches_actual(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:]
            rhs = objects[:]
            last_applied = objects[:]

            match = validate._validate_list_match(
                target=lhs, actual=rhs, last_applied_value=last_applied
            )
            self.assertFalse(match.differences, f"{label} mismatch")
            self.assertTrue(match.match, f"{label} mismatch")

    def test_last_applied_too_short(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:]
            rhs = objects[:]
            last_applied = objects[:2]

            match = validate._validate_list_match(
                target=lhs, actual=rhs, last_applied_value=last_applied
            )
            self.assertFalse(match.differences, f"{label} mismatch")
            self.assertTrue(match.match, f"{label} mismatch")


class TestSetValidator(unittest.TestCase):
    def test_match(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:]
            random.shuffle(lhs)

            rhs = objects[:]
            random.shuffle(rhs)

            match = validate._validate_set_match(target=lhs, actual=rhs)
            self.assertFalse(match.differences, f"{label} mismatch")
            self.assertTrue(match.match, f"{label} mismatch")

    def test_partial_match(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            random.shuffle(objects)
            lhs = objects[: random.randint(3, len(objects) - 2)]

            random.shuffle(objects)
            rhs = objects[: random.randint(3, len(objects) - 2)]

            match = validate._validate_set_match(target=lhs, actual=rhs)
            self.assertTrue(match.differences, f"{label} mismatch")
            self.assertFalse(match.match, f"{label} mismatch")

    def test_extra_values(self):
        generators = (
            ("strings", lambda: string_generator(random.randint(5, 25))),
            ("ints", lambda: random.randint(-500000000, 500000000)),
        )

        for label, generator in generators:
            objects = [generator() for _ in range(random.randint(5, 25))]

            lhs = objects[:-3]

            rhs = objects[:]

            match = validate._validate_set_match(target=lhs, actual=rhs)
            self.assertEqual(1, len(match.differences), f"{label} mismatch")
            self.assertFalse(match.match, f"{label} mismatch")

            (difference_message, *_) = match.differences
            self.assertIn("unexpectedly", difference_message, f"{label} mismatch")

    def test_dict_error_message(self):
        objects = [
            {"name": string_generator(20)},
            {"name": string_generator(20)},
            {"name": string_generator(20)},
            {"name": string_generator(20)},
        ]

        lhs = objects[:]
        random.shuffle(lhs)

        rhs = objects[:]
        random.shuffle(rhs)

        match = validate._validate_set_match(target=lhs, actual=rhs)
        self.assertFalse(match.match)

        (difference_message, *_) = match.differences
        self.assertIn("x-koreo-compare-as-map", difference_message)

    def test_list_message(self):
        objects = [
            [
                string_generator(random.randint(5, 50))
                for _ in range(random.randint(5, 50))
            ],
            [
                string_generator(random.randint(5, 50))
                for _ in range(random.randint(5, 50))
            ],
        ]

        lhs = objects[:]
        random.shuffle(lhs)

        rhs = objects[:]
        random.shuffle(rhs)

        match = validate._validate_set_match(target=lhs, actual=rhs)
        self.assertFalse(match.match)

        (difference_message, *_) = match.differences
        self.assertIn("unhashable", difference_message)

    def test_nulls(self):
        non_nulls = [
            string_generator(random.randint(5, 50))
            for _ in range(random.randint(5, 50))
        ]

        match = validate._validate_set_match(target=non_nulls, actual=None)
        self.assertFalse(match.match)
        (difference_message, *_) = match.differences
        self.assertIn("null set", difference_message)

        match = validate._validate_set_match(target=None, actual=non_nulls)
        self.assertFalse(match.match)
        (difference_message, *_) = match.differences
        self.assertIn("null set", difference_message)

        match = validate._validate_set_match(target=None, actual=None)
        self.assertTrue(match.match)
        self.assertFalse(match.differences)


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
            match = validate.validate_match(target=target, actual=actual)
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
            match = validate.validate_match(target=target, actual=actual)
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

        match = validate.validate_match(target=target_list, actual=actual_list)
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

        match = validate.validate_match(target=target_list, actual=actual_list)
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

        match = validate.validate_match(target=target_list, actual=actual_list)
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

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_list_match(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = target_list.copy()

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_list_length_mismatch(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = [1, "two", {"three": 4, "five": 6}]

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertIn("length", "".join(match.differences))
        self.assertFalse(match.match)

    def test_list_mismatch(self):
        target_list = [1, "two", {"three": 4, "five": "six"}, 7.2]
        actual_list = [1, "two", {"three": 4, "five": 6}, 7.2]

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertTrue(match.differences)
        self.assertFalse(match.match)

    def test_compare_as_map(self):
        target_list = {
            "one": 1,
            "two": False,
            "x-koreo-compare-as-map": {"as-map": ["case-name"]},
            "as-map": [
                {"case-name": "three.1", "value": 0},
                {"case-name": "three.2", "value": 1},
                {"case-name": "three.3", "value": True},
                {"case-name": "three.4", "value": False},
            ],
            "four": 7.2,
        }
        actual_list = copy.deepcopy(target_list)
        random.shuffle(actual_list["as-map"])

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_as_set(self):
        target_list = {
            "one": 1,
            "two": False,
            "x-koreo-compare-as-set": ["as-set"],
            "as-set": [
                string_generator(random.randint(5, 50))
                for _ in range(random.randint(5, 50))
            ],
            "four": 7.2,
        }
        actual_list = copy.deepcopy(target_list)
        random.shuffle(actual_list["as-set"])

        match = validate.validate_match(target=target_list, actual=actual_list)
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_last_applied_simple(self):
        target_value = {
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
                    "e": {"x-koreo-compare-last-applied": ["value"], "value": 1},
                },
            },
            "four": 7.2,
        }
        actual_value = copy.deepcopy(target_value)
        actual_value["three"]["three.6"]["e"]["value"] = 2

        last_applied_value = copy.deepcopy(target_value)

        match = validate.validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_value,
        )
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_last_applied_dict_diff(self):
        target_value = {
            "one": 1,
            "two": False,
            "x-koreo-compare-last-applied": ["to-last-applied"],
            "to-last-applied": {
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
        actual_value = copy.deepcopy(target_value)
        actual_value["to-last-applied"] = {
            "totally": 0,
            "different": 1,
            "values": True,
            "go": ["into", "this", "structure"],
        }

        last_applied_value = copy.deepcopy(target_value)

        match = validate.validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_value,
        )
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_last_applied_list_diff(self):
        target_value = {
            "one": 1,
            "two": False,
            "x-koreo-compare-last-applied": ["to-last-applied"],
            "to-last-applied": ["abc", "one", 2, 3],
            "four": 7.2,
        }
        actual_value = copy.deepcopy(target_value)
        del actual_value["to-last-applied"]

        last_applied_value = copy.deepcopy(target_value)

        match = validate.validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_value,
        )
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_last_applied_set_diff(self):
        target_value = {
            "one": 1,
            "two": False,
            "x-koreo-compare-last-applied": ["to-last-applied"],
            "x-koreo-compare-as-set": ["to-last-applied"],
            "to-last-applied": ["abc", "one", 2, 3],
            "four": 7.2,
        }
        actual_value = copy.deepcopy(target_value)
        actual_value["to-last-applied"] = {"one": True}

        last_applied_value = copy.deepcopy(target_value)
        random.shuffle(last_applied_value["to-last-applied"])

        match = validate.validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_value,
        )
        self.assertFalse(match.differences)
        self.assertTrue(match.match)

    def test_compare_celstring_to_none(self):
        target_value = {
            "x-koreo-compare-last-applied": ["to-last-applied"],
            "to-last-applied": celtypes.StringType(string_generator(50)),
        }
        actual_value = copy.deepcopy(target_value)
        actual_value["to-last-applied"] = string_generator(20)

        last_applied_value = None

        match = validate.validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_value,
        )
        self.assertTrue(match.differences)
        self.assertFalse(match.match)
