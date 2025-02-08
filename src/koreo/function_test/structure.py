from typing import NamedTuple, Sequence

from celpy import celtypes

from koreo.cel.prepare import Overlay
from koreo.resource_function.structure import ResourceFunction
from koreo.result import NonOkOutcome, UnwrappedOutcome
from koreo.value_function.structure import ValueFunction


class ExpectOutcome(NamedTuple):
    outcome: NonOkOutcome | None = None


class ExpectReturn(NamedTuple):
    value: dict | None = None


class ExpectResource(NamedTuple):
    resource: dict | None = None


class ExpectDelete(NamedTuple):
    expected: bool = False


Assertion = ExpectOutcome | ExpectResource | ExpectReturn | ExpectDelete


class TestCase(NamedTuple):
    assertion: Assertion

    # Variant test cases do not carry forward.
    variant: bool = False

    # Skip
    skip: bool = False

    # For non-vaiant cases, these updates to inputs carry forward.
    input_overrides: celtypes.MapType | None = None

    # Mutually exclusive
    current_resource: dict | None = None
    resource_overlay: Overlay | None = None

    # Human friendly output
    label: str | None = None


class FunctionTest(NamedTuple):
    function_under_test: UnwrappedOutcome[ResourceFunction | ValueFunction]

    inputs: celtypes.MapType | None
    initial_resource: dict | None

    test_cases: Sequence[TestCase] | None
