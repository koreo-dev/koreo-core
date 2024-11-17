from typing import NamedTuple

from celpy import celtypes

from koreo.function.structure import Function


class FunctionTest(NamedTuple):
    function_under_test: Function

    parent: celtypes.MapType | None
    inputs: celtypes.MapType | None

    expected_resource: celtypes.MapType | None
