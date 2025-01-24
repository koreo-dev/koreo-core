import json
import unittest

from celpy import celtypes

from koreo.cel.encoder import convert_bools, encode_cel


class TestEncodeBools(unittest.TestCase):
    def test_structure(self):
        value = {
            "zero": 0,
            "one": 1,
            celtypes.BoolType(True): celtypes.BoolType(True),
            celtypes.StringType("true"): celtypes.BoolType(True),
            celtypes.BoolType(False): celtypes.BoolType(False),
            celtypes.StringType("false"): celtypes.BoolType(False),
            "nested_map": {
                "zero": 0,
                "one": 1,
                celtypes.BoolType(True): celtypes.BoolType(True),
                celtypes.StringType("true"): celtypes.BoolType(True),
                celtypes.BoolType(False): celtypes.BoolType(False),
                celtypes.StringType("false"): celtypes.BoolType(False),
            },
            "list": [
                "zero",
                0,
                "one",
                1,
                celtypes.BoolType(True),
                celtypes.StringType("true"),
                celtypes.BoolType(False),
                celtypes.StringType("false"),
            ],
            "cel_values": celtypes.MapType(
                {
                    celtypes.StringType("zero"): celtypes.IntType(0),
                    celtypes.StringType("one"): celtypes.IntType(1),
                    celtypes.BoolType(True): celtypes.BoolType(True),
                    celtypes.StringType("true"): celtypes.BoolType(True),
                    celtypes.StringType("false"): celtypes.BoolType(False),
                    celtypes.StringType("nested_map"): celtypes.MapType(
                        {
                            celtypes.StringType("zero"): celtypes.IntType(0),
                            celtypes.StringType("one"): celtypes.IntType(1),
                            celtypes.BoolType(True): celtypes.BoolType(True),
                            celtypes.StringType("true"): celtypes.BoolType(True),
                            celtypes.BoolType(False): celtypes.BoolType(False),
                            celtypes.StringType("false"): celtypes.BoolType(False),
                        }
                    ),
                    celtypes.StringType("list"): celtypes.ListType(
                        [
                            celtypes.StringType("zero"),
                            celtypes.IntType(0),
                            celtypes.StringType("one"),
                            celtypes.IntType(1),
                            celtypes.BoolType(True),
                            celtypes.StringType("true"),
                            celtypes.BoolType(False),
                            celtypes.StringType("false"),
                        ]
                    ),
                }
            ),
        }
        self.maxDiff = None
        self.assertEqual(
            '{"zero": 0, "one": 1, "true": true, "true": true, "false": false, "false": false, "nested_map": {"zero": 0, "one": 1, "true": true, "true": true, "false": false, "false": false}, "list": ["zero", 0, "one", 1, true, "true", false, "false"], "cel_values": {"zero": 0, "one": 1, "true": true, "true": true, "false": false, "nested_map": {"zero": 0, "one": 1, "true": true, "true": true, "false": false, "false": false}, "list": ["zero", 0, "one", 1, true, "true", false, "false"]}}',
            json.dumps(convert_bools(value)),
        )


class TestEncodeCel(unittest.TestCase):
    def test_empty_str(self):
        value = ""
        self.assertEqual('""', encode_cel(value))

    def test_str(self):
        value = "This is a plain old string."
        self.assertEqual('"This is a plain old string."', encode_cel(value))

    def test_str_with_quote(self):
        value = 'This is a "test."'
        self.assertEqual('"""This is a \\\"test.\\\""""', encode_cel(value))  # fmt: skip

    def test_str_with_tripple_quote(self):
        value = 'This is a """test."""'
        self.assertEqual('"""This is a \\\"\\\"\\\"test.\\\"\\\"\\\""""', encode_cel(value))  # fmt: skip

    def test_expression_str(self):
        value = "=1 + 1"
        self.assertEqual("1 + 1", encode_cel(value))

    def test_int(self):
        value = 1010
        self.assertEqual("1010", encode_cel(value))

    def test_int_str(self):
        value = "3213"
        self.assertEqual("3213", encode_cel(value))

    def test_float(self):
        value = 99.3
        self.assertEqual("99.3", encode_cel(value))

    def test_float_str(self):
        value = "72.3"
        self.assertEqual("72.3", encode_cel(value))

    def test_bool(self):
        value = True
        self.assertEqual("true", encode_cel(value))

    def test_list_of_int(self):
        value = [1, 2, 3]
        self.assertEqual("[1,2,3]", encode_cel(value))

    def test_list_of_float(self):
        value = [1.2, 2.1, 8.9, 9.0]
        self.assertEqual("[1.2,2.1,8.9,9.0]", encode_cel(value))

    def test_list_of_bool(self):
        value = [False, True, True, False]
        self.assertEqual("[false,true,true,false]", encode_cel(value))

    def test_flat_dict(self):
        value = {
            "a_string": "testing",
            "a_quoted_string": 'you should "test"',
            "a_tripple_quoted_string": 'you """should""" "test"',
            "an_int": 7,
            "an_int_str": "29",
            "a_float": 82.34,
            "a_float_str": "94.55",
            "bool_true": True,
            "bool_false": False,
            "empty_list": [],
            "complex_list": ["a", 2, "4", 3.2, "53.4", True, False],
            "nested_list": [[1, 2, 3], [True, False, False], ["a", "b", "c"]],
            "cel_expr": "=8 + 3",
            "cel_expr_list": ["=8 + 3", "='a' + 'b'"],
            "cel_expr_list_list": [["=8 + 3", "='a' + 'b'"], ["=has(value)"]],
            "false": False,
            "true": True,
            "yes": True,
            "no": False,
            "none": None,
            "null": "=null",
        }

        # fmt: off
        self.maxDiff = None
        self.assertEqual(
            '{"a_string":"testing","a_quoted_string":"""you should \\\"test\\\"""","a_tripple_quoted_string":"""you \\\"\\\"\\\"should\\\"\\\"\\\" \\\"test\\\"""","an_int":7,"an_int_str":29,"a_float":82.34,"a_float_str":94.55,"bool_true":true,"bool_false":false,"empty_list":[],"complex_list":["a",2,4,3.2,53.4,true,false],"nested_list":[[1,2,3],[true,false,false],["a","b","c"]],"cel_expr":8 + 3,"cel_expr_list":[8 + 3,\'a\' + \'b\'],"cel_expr_list_list":[[8 + 3,\'a\' + \'b\'],[has(value)]],"false":false,"true":true,"yes":true,"no":false,"none":null,"null":null}',
            encode_cel(value),
        )
        # fmt: on

    def test_nested_dict(self):
        value = {
            "strings": {
                "a_string": "testing",
                "a_quoted_string": '"testing" is important',
                "an_int_str": "29",
                "a_float_str": "94.55",
            },
            "numbers": {"an_int": 7, "a_float": 82.34},
            "bools": {"bool_true": True, "bool_false": False},
            "lists": {
                "empty_list": [],
                "complex_list": ["a", 2, "4", 3.2, "53.4", True, False],
                "nested_list": [[1, 2, 3], [True, False, False], ["a", "b", "c"]],
            },
            "cel": {
                "cel_expr": "=8 + 3",
                "cel_expr_list": ["=8 + 3", "='a' + 'b'"],
                "cel_expr_list_list": [["=8 + 3", "='a' + 'b'"], ["=has(value)"]],
            },
            "dicts": {
                "level_one": {
                    "level_two": {"value": "deep", "cel": "=has(formula)"},
                    "sibling": "=8 + 99",
                },
                "sibling": "a",
            },
        }
        self.maxDiff = None
        # fmt: off
        self.assertEqual(
            '{"strings":{"a_string":"testing","a_quoted_string":"""\\\"testing\\\" is important""","an_int_str":29,"a_float_str":94.55},"numbers":{"an_int":7,"a_float":82.34},"bools":{"bool_true":true,"bool_false":false},"lists":{"empty_list":[],"complex_list":["a",2,4,3.2,53.4,true,false],"nested_list":[[1,2,3],[true,false,false],["a","b","c"]]},"cel":{"cel_expr":8 + 3,"cel_expr_list":[8 + 3,\'a\' + \'b\'],"cel_expr_list_list":[[8 + 3,\'a\' + \'b\'],[has(value)]]},"dicts":{"level_one":{"level_two":{"value":"deep","cel":has(formula)},"sibling":8 + 99},"sibling":"a"}}',
            encode_cel(value),
        )
        # fmt: on
