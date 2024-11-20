import unittest

import celpy

from koreo.cel.structure_extractor import extract_argument_structure


class TestArgumentStructureExtractor(unittest.TestCase):
    def test_simple_args(self):
        env = celpy.Environment()

        cel_str = """{
            "simple_dot": steps.simple1,
            "simple_index": steps['simple2'],
            "nested_dots": steps.nested1.one,
            "nested_index_dot": steps['nested2'].two,
            "nested_index_index": steps['nested3']['one'],
            "nested_dot_index": steps.nested4['one']
        }
        """

        result = extract_argument_structure(env.compile(cel_str))

        sorted_result = sorted(result)

        expected = sorted(
            [
                "steps.nested1",
                "steps.nested2",
                "steps.nested3",
                "steps.nested4",
                "steps.simple1",
                "steps.simple2",
                "steps.nested1.one",
                "steps.nested2.two",
                "steps.nested3.one",
                "steps.nested4.one",
            ]
        )

        self.assertListEqual(expected, sorted_result)

    def test_numeric_indexes(self):
        env = celpy.Environment()

        cel_str = """{
            "simple_index": steps[0],
            "nested_index_dot": steps[2].two,
            "nested_index_index": steps[3][1],
            "nested_dot_index": steps.nested4[4]
        }
        """

        result = extract_argument_structure(env.compile(cel_str))

        sorted_result = sorted(result)

        expected = sorted(
            [
                "steps.0",
                "steps.2",
                "steps.2.two",
                "steps.3",
                "steps.3.1",
                "steps.nested4",
                "steps.nested4.4",
            ]
        )

        self.assertListEqual(expected, sorted_result)

    def test_formula_args(self):
        env = celpy.Environment()

        cel_str = """{
            "simple_add": steps.simple1 + steps['simple2'],
            "nested_add": steps.nested1.one + steps['nested2'].two + steps['nested3']['one'] + steps.nested4['one']
        }
        """

        result = extract_argument_structure(env.compile(cel_str))

        sorted_result = sorted(result)

        expected = sorted(
            [
                "steps.nested1",
                "steps.nested2",
                "steps.nested3",
                "steps.nested4",
                "steps.simple1",
                "steps.simple2",
                "steps.nested1.one",
                "steps.nested2.two",
                "steps.nested3.one",
                "steps.nested4.one",
            ]
        )

        self.assertListEqual(expected, sorted_result)
