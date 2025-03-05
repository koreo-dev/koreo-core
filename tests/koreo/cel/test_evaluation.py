import json
import pprint
import random
import string
import unittest


import celpy
from celpy import celtypes

from koreo.result import PermFail

from koreo.cel.encoder import CEL_PREFIX, convert_bools
from koreo.cel import prepare
from koreo.cel import evaluation

base_object = celpy.json_to_cel(
    {
        "list": [1, 2, 3],
        "string": "This is a test",
        "none": None,
        "int": 17,
        "zero": 0,
        "one": 1,
        "bool": True,
        "true": True,
        "false": False,
        "mapping": {
            "list": [1, 2, 3],
            "string": "This is a test",
            "int": 17,
            "true": True,
            "false": True,
            "mapping": {
                "list": [1, 2, 3],
                "string": "This is a test",
                "int": 17,
                "true": True,
                "false": True,
            },
        },
    }
)


def _make_a_string(words: int) -> str:
    generated = " ".join(
        (
            "".join(
                (char if r"\\" != char else r"\\\\")
                for char in random.choices(
                    string.ascii_letters, k=random.randint(3, 15)
                )
            )
        ).strip()
        for _ in range(words)
    )
    return generated.strip().lstrip(CEL_PREFIX)


def _perform_overlay(overlay_spec, failureException):
    # Why is this needed here?
    assert isinstance(base_object, celtypes.MapType)

    cel_env = celpy.Environment()

    match prepare.prepare_overlay_expression(
        cel_env=cel_env, spec=overlay_spec, location="unit-test-prepare"
    ):
        case None:
            raise failureException(f"Failed to prepare overlay {overlay_spec}")
        case PermFail(message=message):
            overlay_dump = pprint.pformat(overlay_spec)
            raise failureException(
                f"PermFail preparing overlay.\nMessage:\n{message}\nOverlay:\n{overlay_dump}"
            )
        case overlay:
            pass

    match evaluation.evaluate_overlay(
        overlay=overlay,
        inputs={},
        base=base_object,
        location="unit-test-evaluation",
    ):
        case PermFail(message=message):
            raise failureException(f"PermFail overlaying {message}")
        case overlaid:
            return convert_bools(overlaid)


class TestEvaluation(unittest.TestCase):
    def test_encodings(self):
        pass

    def test_string_encodings(self):
        cel_env = celpy.Environment()

        random_string = _make_a_string(50)

        match prepare.prepare_expression(
            cel_env=cel_env, spec=random_string, location="unit-test-prepare"
        ):
            case None:
                raise self.failureException(
                    f"Failed to prepare overlay {random_string}"
                )
            case PermFail(message=message):
                raise self.failureException(
                    f"PermFail preparing string.\nMessage:\n{message}\nOverlay:\n{random_string}"
                )
            case prepared_string:
                pass

        match evaluation.evaluate(
            expression=prepared_string,
            inputs={},
            location="unit-test-evaluation",
        ):
            case PermFail(message=message):
                raise self.failureException(
                    f"PermFail evaluating string.\nMessage:\n{message}\nString:\n{random_string}"
                )
            case output_string:
                self.maxDiff = None
                print(f"End Quote: {output_string[-1] == '"'}")
                self.assertEqual(random_string, output_string)
                # raise self.failureException(
                #    f"Ok evaluating string.\nString:\n{output_string}"
                # )


class TestOverlay(unittest.TestCase):
    def test_simple_existing_values(self):
        random_int_value = random.randint(-10000, 10000)
        random_string = _make_a_string(5)
        random_bool = random.randint(0, 100) > 50
        random_list_values = [
            random.randint(-10000000, 10000000) for _ in range(random.randint(1, 10))
        ]

        overlay_spec = {
            "list": random_list_values,
            "int": random_int_value,
            "string": random_string,
            "bool": random_bool,
        }
        overlaid = _perform_overlay(
            overlay_spec=overlay_spec, failureException=self.failureException
        )

        self.maxDiff = None
        self.assertDictEqual(
            overlaid,
            {
                "list": random_list_values,
                "string": random_string,
                "int": random_int_value,
                "none": None,
                "zero": 0,
                "one": 1,
                "bool": random_bool,
                "true": True,
                "false": False,
                "mapping": {
                    "list": [1, 2, 3],
                    "string": "This is a test",
                    "int": 17,
                    "true": True,
                    "false": True,
                    "mapping": {
                        "list": [1, 2, 3],
                        "string": "This is a test",
                        "int": 17,
                        "true": True,
                        "false": True,
                    },
                },
            },
        )

    def test_simple_new_values(self):
        random_int_value = random.randint(-10000, 10000)
        random_string = _make_a_string(5)
        random_bool = random.randint(0, 100) > 50

        overlay_spec = {
            "new_int": random_int_value,
            "new_string": random_string,
            "new_bool": random_bool,
        }
        overlaid = _perform_overlay(
            overlay_spec=overlay_spec, failureException=self.failureException
        )

        self.maxDiff = None
        self.assertDictEqual(
            overlaid,
            {
                "new_int": random_int_value,
                "new_string": random_string,
                "new_bool": random_bool,
                "list": [1, 2, 3],
                "string": "This is a test",
                "int": 17,
                "none": None,
                "zero": 0,
                "one": 1,
                "bool": True,
                "true": True,
                "false": False,
                "mapping": {
                    "list": [1, 2, 3],
                    "string": "This is a test",
                    "int": 17,
                    "true": True,
                    "false": True,
                    "mapping": {
                        "list": [1, 2, 3],
                        "string": "This is a test",
                        "int": 17,
                        "true": True,
                        "false": True,
                    },
                },
            },
        )

    def test_deeper_existing_values(self):
        tier_one_random_int_value = random.randint(-10000, 10000)
        tier_one_random_string = _make_a_string(5)
        tier_one_random_bool = random.randint(0, 100) > 50
        tier_one_random_list_values = [
            random.randint(-10000000, 10000000) for _ in range(random.randint(1, 10))
        ]

        tier_two_random_int_value = random.randint(-10000, 10000)
        tier_two_random_string = _make_a_string(5)
        tier_two_random_bool = random.randint(0, 100) > 50
        tier_two_random_list_values = [
            random.randint(-10000000, 10000000) for _ in range(random.randint(1, 10))
        ]

        tier_three_random_int_value = random.randint(-10000, 10000)
        tier_three_random_string = _make_a_string(5)
        tier_three_random_bool = random.randint(0, 100) > 50
        tier_three_random_list_values = [
            random.randint(-10000000, 10000000) for _ in range(random.randint(1, 10))
        ]

        overlay_spec = {
            "list": tier_one_random_list_values,
            "int": tier_one_random_int_value,
            "string": tier_one_random_string,
            "bool": tier_one_random_bool,
            "mapping": {
                "list": tier_two_random_list_values,
                "int": tier_two_random_int_value,
                "string": tier_two_random_string,
                "bool": tier_two_random_bool,
                "mapping": {
                    "list": tier_three_random_list_values,
                    "int": tier_three_random_int_value,
                    "string": tier_three_random_string,
                    "bool": tier_three_random_bool,
                },
            },
        }
        overlaid = _perform_overlay(
            overlay_spec=overlay_spec, failureException=self.failureException
        )

        self.maxDiff = None
        self.assertDictEqual(
            overlaid,
            {
                "list": tier_one_random_list_values,
                "string": tier_one_random_string,
                "int": tier_one_random_int_value,
                "none": None,
                "zero": 0,
                "one": 1,
                "bool": tier_one_random_bool,
                "true": True,
                "false": False,
                "mapping": {
                    "list": tier_two_random_list_values,
                    "string": tier_two_random_string,
                    "int": tier_two_random_int_value,
                    "bool": tier_two_random_bool,
                    "true": True,
                    "false": True,
                    "mapping": {
                        "list": tier_three_random_list_values,
                        "string": tier_three_random_string,
                        "int": tier_three_random_int_value,
                        "bool": tier_three_random_bool,
                        "true": True,
                        "false": True,
                    },
                },
            },
        )

    def test_deep_new_values(self):
        random_int_value = random.randint(-10000, 10000)
        random_string = _make_a_string(5)
        random_bool = random.randint(0, 100) > 50

        overlay_spec = {
            "a": {
                "new": {
                    "deepList": [
                        {
                            "new_int": random_int_value,
                            "new_string": random_string,
                            "new_bool": random_bool,
                        }
                    ],
                    "deepMap": {
                        "new_int": random_int_value,
                        "new_string": random_string,
                        "new_bool": random_bool,
                    },
                }
            }
        }
        overlaid = _perform_overlay(
            overlay_spec=overlay_spec, failureException=self.failureException
        )

        self.maxDiff = None
        self.assertDictEqual(
            overlaid,
            {
                "a": {
                    "new": {
                        "deepList": [
                            {
                                "new_int": random_int_value,
                                "new_string": random_string,
                                "new_bool": random_bool,
                            }
                        ],
                        "deepMap": {
                            "new_int": random_int_value,
                            "new_string": random_string,
                            "new_bool": random_bool,
                        },
                    }
                },
                "list": [1, 2, 3],
                "string": "This is a test",
                "int": 17,
                "none": None,
                "zero": 0,
                "one": 1,
                "bool": True,
                "true": True,
                "false": False,
                "mapping": {
                    "list": [1, 2, 3],
                    "string": "This is a test",
                    "int": 17,
                    "true": True,
                    "false": True,
                    "mapping": {
                        "list": [1, 2, 3],
                        "string": "This is a test",
                        "int": 17,
                        "true": True,
                        "false": True,
                    },
                },
            },
        )
