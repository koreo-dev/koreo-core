from typing import NamedTuple

from celpy import celtypes

from koreo.result import Outcome, UnwrappedOutcome
from koreo.function.structure import Function
from koreo.value_function.structure import ValueFunction


class FunctionTest(NamedTuple):
    function_under_test: UnwrappedOutcome[ValueFunction | Function]

    inputs: celtypes.MapType | None

    expected_outcome: Outcome | None
    expected_return: dict | None

    current_resource: dict | None
    expected_resource: dict | None
