import copy
import json
import random
import string
import unittest

import celpy
import jsonpath_ng

from koreo.cel.encoder import encode_cel, encode_cel_template

from koreo.result import Ok

from koreo.function import structure as function_structure
from koreo.function import reconcile


class TestReconcileFunction(unittest.IsolatedAsyncioTestCase):
    def test_overlay(self):
        base = {}

        cel_env = celpy.Environment()

        overlay = celpy.CELJSONEncoder.to_python(
            cel_env.program(
                cel_env.compile(
                    encode_cel(
                        json.loads(
                            '{"metadata.name": "test-case", "spec": {"boolTrue": true, "boolFalse": false}}'
                        )
                    )
                )
            ).evaluate({})
        )

        for field_path, value in overlay.items():
            field_expr = jsonpath_ng.parse(field_path)
            field_expr.update_or_create(base, value)

        self.assertDictEqual(
            {
                "metadata": {"name": "test-case"},
                "spec": {"boolTrue": True, "boolFalse": False},
            },
            base,
        )
        self.assertIsInstance(base.get("spec").get("boolTrue"), bool)
        self.assertIsInstance(base.get("spec").get("boolFalse"), bool)

    async def test_reconcile(self):
        cel_env = celpy.Environment()

        inputs = cel_env.program(cel_env.compile(encode_cel({"value": 7}))).evaluate({})

        input_validators = cel_env.program(
            cel_env.compile(
                f"{encode_cel([{'type': 'Skip', 'message': 'Bad input', 'test': '=!has(inputs.value)'}])}.filter(predicate, predicate.test)"
            )
        )

        source_ok_value = cel_env.program(
            cel_env.compile(
                encode_cel(
                    json.loads(
                        json.dumps(
                            {
                                "resources": [
                                    # {"bool": True},
                                    # {"bool": False},
                                    {"int": 18},
                                    {"float": 1.2},
                                    {"str": "some string"},
                                ]
                            }
                        )
                    )
                )
            )
        )

        function = function_structure.Function(
            resource_config=function_structure.StaticResource(
                behavior=function_structure.Behavior(
                    load="virtual",
                    create=False,
                    update="never",
                    delete="abandon",
                ),
                managed_resource=None,
            ),
            input_validators=input_validators,
            outcome=function_structure.Outcome(tests=None, ok_value=source_ok_value),
            materializers=function_structure.Materializers(base=None, on_create=None),
        )

        result = await reconcile.reconcile_function(
            api=None, location="unittest", function=function, trigger={}, inputs=inputs
        )

        self.assertEqual(
            json.dumps(
                {
                    "resources": [
                        # {"bool": True},
                        # {"bool": False},
                        {"int": 18},
                        {"float": 1.2},
                        {"str": "some string"},
                    ]
                }
            ),
            json.dumps(celpy.CELJSONEncoder.to_python(result)),
        )


class TestValidateMatch(unittest.TestCase):
    def test_int_match(self):
        matching_int = random.randint(-1000, 1000)
        self.assertTrue(reconcile._validate_match(matching_int, matching_int))

    def test_int_mismatch(self):
        lower = -10000
        upper = 10000
        int_1 = random.randint(lower, upper)
        int_2 = random.randint(lower, upper)
        if int_1 == int_2:
            int_2 = int_2 + 1
        self.assertFalse(reconcile._validate_match(int_1, int_2))

    def test_str_match(self):
        randstr = "".join(
            (random.choice(string.printable) for _ in range(random.randint(10, 1000)))
        )
        self.assertTrue(reconcile._validate_match(randstr, randstr))

    def test_str_mismatch(self):
        randstr_1 = "".join(
            (random.choice(string.printable) for _ in range(random.randint(10, 1000)))
        )
        randstr_2 = "".join(
            (random.choice(string.printable) for _ in range(random.randint(10, 1000)))
        )
        self.assertFalse(reconcile._validate_match(randstr_1, randstr_2))

    def test_str_int_no_match(self):
        # No form of coercion currently exists, so this is a simple test.
        randint = random.randint(10, 10000)
        self.assertFalse(reconcile._validate_match(randint, str(randint)))

    def test_lists_match(self):
        input_list_1 = [
            "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for _ in range(random.randint(10, 100))
        ]
        input_list_2 = copy.deepcopy(input_list_1)
        self.assertTrue(reconcile._validate_match(input_list_1, input_list_2))

    def test_lists_mismatch(self):
        input_list_1 = [
            "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for _ in range(random.randint(10, 100))
        ]
        input_list_2 = [
            "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for _ in range(random.randint(10, 100))
        ]
        self.assertFalse(reconcile._validate_match(input_list_1, input_list_2))

    def test_same_length_lists_mismatch(self):
        input_list_1 = [
            "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for _ in range(10)
        ]
        input_list_2 = [
            "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for _ in range(10)
        ]
        self.assertFalse(reconcile._validate_match(input_list_1, input_list_2))

    def test_dict_match(self):
        input_dict_1 = {
            f"key-{key}": "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for key in range(random.randint(10, 100))
        }

        input_dict_2_keys = copy.deepcopy(list(input_dict_1.keys()))
        random.shuffle(input_dict_2_keys)

        input_dict_2 = {key: input_dict_1[key] for key in input_dict_2_keys}

        self.assertTrue(reconcile._validate_match(input_dict_1, input_dict_2))

    def test_dict_extra_in_actual_match(self):
        input_dict_1 = {
            f"key-{key}": "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for key in range(random.randint(10, 100))
        }

        input_dict_2 = copy.deepcopy(input_dict_1)
        input_dict_2.update(
            {
                "".join(
                    (
                        random.choice(string.printable)
                        for _ in range(random.randint(10, 100))
                    )
                ): "".join(
                    (
                        random.choice(string.printable)
                        for _ in range(random.randint(10, 100))
                    )
                )
                for _ in range(10)
            }
        )

        from pprint import pprint

        pprint(input_dict_1)
        pprint(input_dict_2)

        self.assertTrue(reconcile._validate_match(input_dict_1, input_dict_2))

    def test_dict_mismatch(self):
        input_dict_1 = {
            f"key-{key}": "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for key in range(random.randint(10, 100))
        }
        input_dict_2 = {
            f"key-{key}": "".join(
                (
                    random.choice(string.printable)
                    for _ in range(random.randint(10, 100))
                )
            )
            for key in range(random.randint(10, 100))
        }
        self.assertFalse(reconcile._validate_match(input_dict_1, input_dict_2))
