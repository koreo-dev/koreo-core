from typing import NamedTuple

from celpy import celtypes

from koreo.result import Outcome
from koreo.function.structure import Function


class FunctionTest(NamedTuple):
    function_under_test: Function

    inputs: celtypes.MapType | None

    expected_outcome: Outcome | None
    expected_ok_value: dict | None

    current_resource: dict | None
    expected_resource: dict | None
