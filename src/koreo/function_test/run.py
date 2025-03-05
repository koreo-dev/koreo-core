from __future__ import annotations
from contextlib import asynccontextmanager
from typing import NamedTuple, Sequence
import copy
import json

import kr8s

import celpy
from celpy import celtypes

from constants import LAST_APPLIED_ANNOTATION

from koreo import result
from koreo.cel.encoder import convert_bools
from koreo.cel.evaluation import evaluate_overlay
from koreo.cel.functions import _overlay

from koreo.resource_function.reconcile import reconcile_resource_function
from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.reconcile import reconcile_value_function
from koreo.value_function.structure import ValueFunction

from .structure import (
    ExpectDelete,
    ExpectOutcome,
    ExpectResource,
    ExpectReturn,
    FunctionTest,
    TestCase,
)


class MockResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class MockApi:
    def __init__(self, current_resource: dict | None, *args, **kwargs):
        self._current_resource = current_resource
        self._materialized = None
        self._api_called = False
        self._delete_called = False

    @property
    def materialized(self):
        return self._materialized

    @property
    def namespace(self):
        return "FAKE-NAMESPACE"

    async def async_get(self, *args, **kwargs):
        if self._current_resource:
            # TODO: This should probably be loaded and built from the Function,
            # using _build_resource_config

            resource_class = kr8s.objects.new_class(
                version=self._current_resource.get("apiVersion"),
                kind=self._current_resource.get("kind"),
                namespaced=(
                    True
                    if self._current_resource.get("metadata", {}).get("namespace")
                    else False
                ),
                asyncio=True,
            )
            yield resource_class(
                api=self,
                resource=self._current_resource,
                namespace=self._current_resource.get("metadata", {}).get("namespace"),
            )

    @asynccontextmanager
    async def call_api(self, *args, **kwargs):
        self._api_called = True

        if args and "DELETE" in args:
            self._materialized = {}
            self._delete_called = True
            yield MockResponse(data=None)
            return

        data = json.loads(kwargs.get("data", "{}"))

        if not self._current_resource:
            merged = data
        else:
            merged = _merge_overlay(self._current_resource, data)

        self._materialized = merged
        yield MockResponse(data=merged)


def _merge_overlay(base, overlay):
    updated = copy.deepcopy(base)
    for key, value in overlay.items():
        match value:
            case dict():
                match base.get(key):
                    case dict() as sub_base:
                        updated[key] = _merge_overlay(sub_base, value)

        updated[key] = value

    return updated


class TestCaseResult(NamedTuple):
    test_pass: bool

    expected_outcome: result.Outcome | None = None
    outcome: result.UnwrappedOutcome | None = None

    message: str | None = None
    differences: Difference | None = None

    label: str | None = None


class TestResult(NamedTuple):
    test_results: Sequence[TestCaseResult]
    function_healthy: result.UnwrappedOutcome
    fatal_error: bool = False


class TestCaseOutcome(NamedTuple):
    new_inputs: celtypes.MapType | None
    new_resource: dict | None
    result: TestCaseResult
    fatal_error: bool = False


async def run_function_test(location: str, function_test: FunctionTest) -> TestResult:
    if not result.is_unwrapped_ok(function_test.function_under_test):
        return TestResult(
            test_results=[],
            function_healthy=function_test.function_under_test,
            fatal_error=True,
        )

    if not function_test.test_cases:
        return TestResult(test_results=[], function_healthy=result.Ok(None))

    test_results, fatal_error = await _run_test_cases(
        location=location,
        function_under_test=function_test.function_under_test,
        base_inputs=function_test.inputs,
        initial_resource=function_test.initial_resource,
        test_cases=function_test.test_cases,
    )
    return TestResult(
        test_results=test_results,
        function_healthy=result.Ok(None),
        fatal_error=fatal_error,
    )


async def _run_test_cases(
    location: str,
    function_under_test: ResourceFunction | ValueFunction,
    base_inputs: celtypes.MapType | None,
    initial_resource: dict | None,
    test_cases: Sequence[TestCase],
) -> tuple[Sequence[TestCaseResult], bool]:
    current_inputs = base_inputs
    current_resource = initial_resource

    test_results: list[TestCaseResult] = []
    for idx, test_case in enumerate(test_cases):
        result = await _run_test_case(
            location=location,
            current_resource=copy.deepcopy(current_resource),
            function_under_test=function_under_test,
            base_inputs=copy.deepcopy(current_inputs),
            test_case=test_case,
            idx=idx,
        )

        current_resource = result.new_resource
        current_inputs = result.new_inputs

        test_results.append(result.result)

        if result.fatal_error:
            return test_results, True

    return test_results, False


async def _run_test_case(
    location: str,
    function_under_test: ResourceFunction | ValueFunction,
    current_resource: dict | None,
    base_inputs: celtypes.MapType | None,
    test_case: TestCase,
    idx: int,
) -> TestCaseOutcome:
    location = f"{location}.testCases[{idx}]"
    if test_case.skip:
        return TestCaseOutcome(
            new_inputs=base_inputs,
            new_resource=current_resource,
            result=TestCaseResult(
                test_pass=True,
                expected_outcome=None,
                outcome=None,
                message=f"user skipped",
                label=test_case.label,
            ),
            fatal_error=False,
        )

    if base_inputs and test_case.input_overrides:
        inputs = _overlay(base_inputs, test_case.input_overrides)
        if isinstance(inputs, celpy.CELEvalError):
            return TestCaseOutcome(
                new_inputs=base_inputs,
                new_resource=current_resource,
                result=TestCaseResult(
                    test_pass=False,
                    expected_outcome=None,
                    outcome=None,
                    message=f"Could not run test case due to inputs overlay error: ({inputs})",
                    label=test_case.label,
                ),
                fatal_error=not test_case.variant,
            )

    elif base_inputs:
        inputs = base_inputs
    elif test_case.input_overrides:
        inputs = test_case.input_overrides
    else:
        inputs = celtypes.MapType()

    if test_case.resource_overlay:
        if not current_resource:
            return TestCaseOutcome(
                new_inputs=base_inputs,
                new_resource=current_resource,
                result=TestCaseResult(
                    test_pass=False,
                    expected_outcome=None,
                    outcome=None,
                    message="Can not overlay until the full resource exists. Try setting `currentResource` or reordering test cases.",
                    label=test_case.label,
                ),
                fatal_error=True,
            )

        match evaluate_overlay(
            overlay=test_case.resource_overlay,
            inputs=inputs,
            base=current_resource,
            location=f"{test_case.label}:overlayResource",
        ):
            case result.PermFail() as err:
                return TestCaseOutcome(
                    new_inputs=base_inputs,
                    new_resource=current_resource,
                    result=TestCaseResult(
                        test_pass=False,
                        expected_outcome=None,
                        outcome=err,
                        message=f"Could not run test case due to resource overlay error: ({err})",
                        label=test_case.label,
                    ),
                    fatal_error=not test_case.variant,
                )
            case _ as cel_resource:
                resource = convert_bools(cel_resource)

    elif test_case.current_resource:
        resource = test_case.current_resource
    else:
        resource = current_resource

    api = MockApi(current_resource=resource)

    match function_under_test:
        case ResourceFunction():
            reconcile_result, _ = await reconcile_resource_function(
                api=api,
                location=location,
                function=function_under_test,
                owner=(
                    f"{function_under_test.crud_config.resource_api.namespace}",
                    celtypes.MapType({"uid": "unit-test-123"}),
                ),
                inputs=inputs,
            )
        case ValueFunction():
            if not resource:
                base_resource = celtypes.MapType({})
            else:
                base_resource = celpy.json_to_cel(resource)
                assert isinstance(base_resource, celtypes.MapType)

            reconcile_result = await reconcile_value_function(
                location=location,
                function=function_under_test,
                inputs=inputs,
                value_base=base_resource,
            )

    match test_case.assertion:
        case ExpectOutcome(outcome=expected_outcome):
            outcome_result = _validate_outcome_match(
                expected=expected_outcome, actual=reconcile_result
            )
        case ExpectReturn(value=expected_value):
            outcome_result = _validate_return_match(
                expected=expected_value, actual=reconcile_result
            )
        case ExpectResource(resource=expected_resource):
            outcome_result = _validate_resource_match(
                expected=expected_resource,
                materialized=api.materialized,
                actual_outcome=reconcile_result,
            )
        case ExpectDelete(expected=expected):
            passed = api._delete_called == expected

            if api._delete_called:
                message = "Delete called"
            else:
                message = "Delete not called."

            outcome_result = TestCaseResult(
                test_pass=passed,
                expected_outcome=result.Retry(delay=0, message=""),
                outcome=reconcile_result,
                message=message,
            )

    if test_case.variant:
        return TestCaseOutcome(
            new_inputs=base_inputs,
            new_resource=current_resource,
            result=copy.replace(outcome_result, label=test_case.label),
        )

    return TestCaseOutcome(
        new_inputs=inputs,
        new_resource=api.materialized if api._api_called else resource,
        result=copy.replace(outcome_result, label=test_case.label),
        fatal_error=not outcome_result.test_pass,
    )


def _validate_return_match(
    expected: dict | None, actual: result.UnwrappedOutcome[celtypes.Value] | None
):
    if not result.is_unwrapped_ok(actual):
        return TestCaseResult(
            test_pass=False,
            expected_outcome=result.Ok(expected),
            outcome=actual,
            message=f"Non-Ok return value ({actual})",
        )

    converted_actual = convert_bools(actual)

    match = _validate_match(target=expected, actual=converted_actual)
    return TestCaseResult(
        test_pass=match.match,
        expected_outcome=result.Ok(expected),
        outcome=converted_actual,
        differences=match.differences,
    )


def _validate_resource_match(
    expected: dict | None,
    materialized: dict | None,
    actual_outcome: result.UnwrappedOutcome[celtypes.Value] | None,
):
    if materialized is None and expected is not None:
        if actual_outcome and not result.is_unwrapped_ok(actual_outcome):
            outcome_message = actual_outcome.message
        else:
            outcome_message = "unexpected Ok result"

        return TestCaseResult(
            test_pass=False,
            expected_outcome=result.Ok(expected),
            outcome=actual_outcome,
            message=f"No resource changes attempted: {outcome_message}",
        )

    if not isinstance(actual_outcome, result.Retry):
        return TestCaseResult(
            test_pass=False,
            expected_outcome=result.Ok(expected),
            outcome=actual_outcome,
            message=f"Unexpected result-type ({actual_outcome}), but expected Retry",
        )

    materialized_without_last_applied = _strip_last_applied_annotation(materialized)

    match = _validate_match(target=expected, actual=materialized_without_last_applied)
    return TestCaseResult(
        test_pass=match.match,
        expected_outcome=result.Ok(expected),
        outcome=actual_outcome,
        differences=match.differences,
    )


def _validate_outcome_match(
    expected: result.Outcome | None,
    actual: result.UnwrappedOutcome[celtypes.Value] | None,
):
    test_pass = False
    message = None

    match (expected, actual):
        case result.Retry(message=expected_message, delay=expected_delay), result.Retry(
            message=actual_message, delay=actual_delay
        ):
            if not expected_message:
                test_pass = True
            elif actual_message and (
                expected_message.lower() in actual_message.lower()
            ):
                test_pass = True

            if expected_delay and (expected_delay != actual_delay):
                # test_pass must be True here, so it needs set to False on mismatch
                test_pass = False

            if not test_pass:
                message = (
                    "Retry: "
                    f"Expected(message={expected_message}, delay={expected_delay}) "
                    f"Actual(message={actual_message}, delay={actual_delay})"
                )

        case result.PermFail(message=expected_message), result.PermFail(
            message=actual_message
        ):
            if not expected_message:
                test_pass = True
            elif actual_message and (
                expected_message.lower() in actual_message.lower()
            ):
                test_pass = True
            else:
                message = (
                    "PermFail: "
                    f"Expected(message={expected_message}) "
                    f"Actual(message={actual_message})"
                )

        case result.DepSkip(message=expected_message), result.DepSkip(
            message=actual_message
        ):
            if not expected_message:
                test_pass = True
            elif actual_message and (
                expected_message.lower() in actual_message.lower()
            ):
                test_pass = True
            else:
                message = (
                    "DepSkip: "
                    f"Expected(message={expected_message}) "
                    f"Actual(message={actual_message})"
                )

        case result.Skip(message=expected_message), result.Skip(message=actual_message):
            if not expected_message:
                test_pass = True
            elif actual_message and (
                expected_message.lower() in actual_message.lower()
            ):
                test_pass = True
            else:
                message = (
                    "Skip: "
                    f"Expected(message={expected_message}) "
                    f"Actual(message={actual_message})"
                )

        case _, _:
            if actual and result.is_unwrapped_ok(actual):
                actual = convert_bools(actual)
                actual_str = f"Ok({actual})"
            else:
                actual_str = f"{actual}"

            if expected and result.is_ok(expected):
                if not expected.data:
                    test_pass = True
                else:
                    match = _validate_match(expected.data, actual)
                    test_pass = match.match
                    if not test_pass:
                        message = f"Mismatch at {" -> ".join(match.differences)}"
            elif expected is None:
                test_pass = result.is_unwrapped_ok(actual)
                if not test_pass:
                    message = f"Expected(Ok) Actual({actual_str})"

            else:
                message = f"Expected({expected}) Actual({actual_str})"

    return TestCaseResult(
        test_pass=test_pass,
        expected_outcome=expected,
        outcome=actual,
        message=message,
    )


Difference = dict[str, "str | Difference"] | Sequence["Difference | None"] | str


class ResourceMatch(NamedTuple):
    match: bool
    differences: Difference


def _strip_last_applied_annotation[T](actual: T) -> T:
    if not actual or not isinstance(actual, dict):
        return actual

    if "metadata" not in actual:
        return actual

    if "annotations" not in actual["metadata"]:
        return actual

    annotation_count = len(actual["metadata"]["annotations"])
    if annotation_count == 0:
        return actual

    stripped = copy.deepcopy(actual)
    if annotation_count == 1:
        del stripped["metadata"]["annotations"]
    else:
        del stripped["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]

    return stripped


def _validate_match(target, actual, compare_list_as_set: bool = False) -> ResourceMatch:
    """Compare the specified (`target`) state against the actual (`actual`)
    reosurce state. We compare all target fields and ignore anything extra.
    """
    match (target, actual):
        # Objects need a special comparator
        case dict(), dict():
            return _validate_dict_match(target, actual)
        case dict(), _:
            return ResourceMatch(
                match=False,
                differences=f"expected `object` but found `{type(actual)}`",
            )
        case _, dict():
            return ResourceMatch(
                match=False,
                differences=f"expected `{type(target)}` but found `object`",
            )

        # Arrays need a special comparator
        case (list() | tuple(), list() | tuple()):
            if compare_list_as_set:
                return _validate_set_match(target, actual)

            return _validate_list_match(target, actual)

        case (list() | tuple(), _):
            return ResourceMatch(
                match=False,
                differences=f"expected `array` but found `{type(actual)}`",
            )
        case (_, list() | tuple()):
            return ResourceMatch(
                match=False,
                differences=f"expected `{type(target)}` but found `array`",
            )

        # Bool needs a special comparator, due to Python's int truthiness rules
        case bool(), bool():
            if target == actual:
                return ResourceMatch(match=True, differences=())
            else:
                return ResourceMatch(
                    match=False,
                    differences=f"expected `{target}` but found `{actual}`",
                )
        case bool(), _:
            return ResourceMatch(
                match=False,
                differences=f"expected `bool` but found `{type(actual)}`",
            )
        case _, bool():
            return ResourceMatch(
                match=False,
                differences=f"expected `{type(target)}` but found `bool`",
            )

    # Hopefully anything else is a simple type.
    if target == actual:
        return ResourceMatch(match=True, differences=())

    return ResourceMatch(
        match=False,
        differences=f"expected '`{target}`' but found '`{actual}`'",
    )


KOREO_DIRECTIVE_KEYS: set[str] = {
    "x-koreo-compare-as-set",
    "x-koreo-compare-as-map",
}


def _obj_to_key(obj: dict, fields: Sequence[int | str]) -> str:
    return "$".join(f"{obj.get(field)}".strip() for field in fields)


def _list_to_object(
    obj_list: Sequence[dict], key_fields: Sequence[int | str]
) -> dict[str, dict]:
    return {_obj_to_key(obj, key_fields): obj for obj in obj_list}


def _validate_dict_match(target: dict, actual: dict) -> ResourceMatch:
    differences: dict[str, str | Difference] = {}

    compare_list_as_set_keys = {
        key for key in target.get("x-koreo-compare-as-set", ()) if key
    }

    compare_as_map = {
        key: [field_name for field_name in fields if field_name]
        for key, fields in target.get("x-koreo-compare-as-map", {}).items()
        if key
    }

    target_keys = set(target.keys()) - KOREO_DIRECTIVE_KEYS
    actual_keys = set(actual.keys()) - KOREO_DIRECTIVE_KEYS

    for missing_key in target_keys - actual_keys:
        differences[missing_key] = "missing"

    for unexpected_key in actual_keys - target_keys:
        differences[unexpected_key] = "unexpected"

    for compare_key in target_keys.intersection(actual_keys):
        if compare_key in compare_as_map:
            key_match = _validate_match(
                _list_to_object(target[compare_key], compare_as_map[compare_key]),
                _list_to_object(actual[compare_key], compare_as_map[compare_key]),
            )
        else:
            key_match = _validate_match(
                target[compare_key],
                actual[compare_key],
                compare_list_as_set=(compare_key in compare_list_as_set_keys),
            )

        if not key_match.match:
            differences[compare_key] = key_match.differences

    return ResourceMatch(match=not differences, differences=differences)


def _validate_list_match(target: list | tuple, actual: list | tuple) -> ResourceMatch:
    if len(target) != len(actual):
        return ResourceMatch(
            match=False,
            differences=f"<length mismatch target:{len(target)}, actual:{len(actual)}",
        )

    has_differences = False
    differences = []
    for target_value, actual_value in zip(target, actual):
        item_match = _validate_match(target_value, actual_value)
        if item_match.match:
            differences.append(None)
        else:
            has_differences = True
            differences.append(item_match.differences)

    return ResourceMatch(match=not has_differences, differences=differences)


def _validate_set_match(target: list | tuple, actual: list | tuple) -> ResourceMatch:
    try:
        target_set = set(target)
        actual_set = set(actual)
    except TypeError as err:
        if "dict" in f"{err}":
            return ResourceMatch(
                match=False,
                differences=f"Could not compare array-as-set, try: `x-koreo-compare-as-map`",
            )

        return ResourceMatch(
            match=False, differences=f"Could not compare array-as-set ({err})"
        )
    except Exception as err:
        return ResourceMatch(
            match=False, differences=f"Could not compare array-as-set ({err})"
        )

    missing_values = target_set - actual_set
    unexpected_values = actual_set - target_set

    if not (missing_values or unexpected_values):
        return ResourceMatch(match=True, differences=())

    differences = []

    for missing_value in missing_values:
        differences.append(
            f"<missing '{missing_value}'>",
        )

    for unexpected_value in unexpected_values:
        differences.append(
            f"<unexpectedly found '{unexpected_value}'>",
        )

    return ResourceMatch(match=False, differences=differences)
