from typing import Sequence

import celpy
from celpy import celtypes

from koreo import ref_helpers
from koreo import registry
from koreo import schema
from koreo.cache import get_resource_from_cache
from koreo.cel.evaluation import evaluate
from koreo.cel.functions import _overlay, koreo_function_annotations
from koreo.cel.prepare import prepare_overlay_expression
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.result import DepSkip, PermFail, UnwrappedOutcome, is_error, is_unwrapped_ok

from koreo.predicate_helpers import predicate_to_koreo_result
from koreo.resource_function.structure import ResourceFunction, ResourceTemplateRef
from koreo.resource_template.structure import ResourceTemplate

from . import structure


async def prepare_function_test(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.FunctionTest, Sequence[registry.Resource]]]:
    location = f"prepare:FunctionTest:{cache_key}"

    if error := schema.validate(
        resource_type=structure.FunctionTest, spec=spec, validation_required=True
    ):
        return PermFail(
            error.message,
            location=f"{cache_key}:spec",
        )

    match ref_helpers.function_ref_spec_to_resource(
        spec.get("functionRef"),
        location=f"{cache_key}:spec.functionRef",
    ):
        case PermFail() as perm_fail:
            return perm_fail
        case None:
            return PermFail(
                "Missing `spec.functionRef`",
                location=f"{cache_key}:spec.functionRef",
            )
        case registry.Resource() as watched_function:
            # Just needed to set `watched_function`
            pass

    function_under_test = get_resource_from_cache(
        resource_class=watched_function.resource_type, cache_key=watched_function.name
    )
    if not function_under_test:
        function_under_test = DepSkip(
            message=(
                f"{watched_function.resource_type.__name__} ({watched_function.name}) "
                f"not found or not ready, FunctionTest '{cache_key}' must wait."
            ),
            location=location,
        )

    initial_resource = spec.get("currentResource")

    cel_env = celpy.Environment(annotations=koreo_function_annotations)

    test_cases_spec = spec.get("testCases")
    test_cases = _prepare_test_cases(
        cel_env=cel_env,
        spec=test_cases_spec,
        resource_function=watched_function.resource_type == ResourceFunction,
    )

    if is_error(test_cases):
        return PermFail(
            message=test_cases.message,
            location=f"FunctionTest:{cache_key}:{test_cases.location}",
        )

    raw_inputs = spec.get("inputs", {})
    if not raw_inputs:
        inputs = None
    else:
        inputs = celpy.json_to_cel(raw_inputs)
        if not isinstance(inputs, celtypes.MapType):
            return PermFail(
                message=f"FunctionTest '{cache_key}' `spec.inputs` ('{inputs}') must be an object.",
                location=location,
            )

    watched_resources: list[registry.Resource] = [watched_function]

    resource_template_resources = _check_for_resource_template_ref(
        function_under_test, inputs, test_cases
    )
    if resource_template_resources:
        watched_resources.extend(resource_template_resources)

    return (
        structure.FunctionTest(
            function_under_test=function_under_test,
            inputs=inputs,
            test_cases=test_cases,
            initial_resource=initial_resource,
        ),
        watched_resources,
    )


def _prepare_test_cases(
    cel_env: celpy.Environment, spec: list[dict] | None, resource_function: bool
) -> Sequence[structure.TestCase] | PermFail:
    test_cases = []

    if not spec:
        return test_cases

    for idx, test_spec in enumerate(spec):
        test_case = _prepare_test_case(
            cel_env, spec=test_spec, idx=idx, resource_function=resource_function
        )
        if is_error(test_case):
            return test_case

        test_cases.append(test_case)

    return test_cases


def _prepare_test_case(
    cel_env: celpy.Environment, spec: dict, idx: int, resource_function: bool
) -> structure.TestCase | PermFail:
    variant = spec.get("variant", False)

    label = spec.get("label")
    if not label:
        label = f"Test Case {idx + 1}"

    if variant and "variant" not in label.lower():
        label = f"[variant] {label}"

    location = f'spec.testCases[{idx}: "{label}"]'

    # This screwy structure is just due to Python's crappy type narrowing.
    raw_overrides = spec.get("inputOverrides", {})
    if not raw_overrides:
        overrides = None
    else:
        overrides = celpy.json_to_cel(raw_overrides)
        if not isinstance(overrides, celtypes.MapType):
            return PermFail(
                message=f"`{location}.inputOverrides` must be an object.",
                location=f"{location}.inputOverrides",
            )

    current_resource = spec.get("currentResource")
    if current_resource is not None and not isinstance(current_resource, dict):
        return PermFail(
            message=f"`{location}.currentResource` must be an object.",
            location=f"{location}.currentResource",
        )

    overlay_resource = spec.get("overlayResource")
    if overlay_resource is not None and not isinstance(overlay_resource, dict):
        return PermFail(
            message=f"`{location}.overlayResource` must be an object.",
            location=f"{location}.overlayResource",
        )

    match prepare_overlay_expression(
        cel_env=cel_env,
        spec=overlay_resource,
        location=f"{location}.overlayResource",
    ):
        case PermFail() as err:
            return err
        case None:
            overlay = None
        case _ as overlay:
            pass

    # Just used so that there is only one.
    bad_assertions_failure = PermFail(
        message=(
            f"`{location}` must contain exactly one assertion (`expectResource`, `expectReturn`, `expectOutcome`, or `expectDelete`)."
        ),
        location=f"{location}",
    )

    assertion = None
    expected_resource = spec.get("expectResource")
    if expected_resource is not None:
        if not resource_function:
            return PermFail(
                message=f"`{location}.expectResource` only valid for `ResourceFunction` tests.",
                location=f"{location}.expectResource",
            )

        if not isinstance(expected_resource, dict):
            return PermFail(
                message=f"`{location}.expectResource` must be an object.",
                location=f"{location}.expectResource",
            )

        if not expected_resource:
            return PermFail(
                message=f"`{location}.expectResource` may not be an empty object.",
                location=f"{location}.expectResource",
            )

        if assertion:
            return bad_assertions_failure

        assertion = structure.ExpectResource(expected_resource)

    expected_outcome_spec = spec.get("expectOutcome")
    if expected_outcome_spec is not None:
        if not isinstance(expected_outcome_spec, dict):
            return PermFail(
                message=(f"`{location}.expectOutcome` must be an object."),
                location=f"{location}.expectOutcome",
            )

        if not expected_outcome_spec:
            return PermFail(
                message=(f"`{location}.expectOutcome` must be a valid outcome."),
                location=f"{location}.expectOutcome",
            )

        if assertion:
            return bad_assertions_failure

        expected_outcome_spec["assert"] = True
        assertion = structure.ExpectOutcome(
            predicate_to_koreo_result(
                celpy.json_to_cel([expected_outcome_spec]),
                location=f"{location}.expectOutcome",
            )
        )

    expected_return = spec.get("expectReturn")
    if expected_return is not None:
        if not isinstance(expected_return, dict):
            return PermFail(
                message=(f"`{location}.expectReturn` must be an object."),
                location=f"{location}.expectReturn",
            )

        if not expected_return:
            return PermFail(
                message=(f"`{location}.expectReturn` may not be an empty object."),
                location=f"{location}.expectReturn",
            )

        if assertion:
            return bad_assertions_failure

        assertion = structure.ExpectReturn(expected_return)

    expected_delete = spec.get("expectDelete")
    if expected_delete is not None:
        if not resource_function:
            return PermFail(
                message=f"`{location}.expectDelete` only valid for `ResourceFunction` tests.",
                location=f"{location}.expectDelete",
            )

        if assertion:
            return bad_assertions_failure

        assertion = structure.ExpectDelete(expected_delete)

    if not assertion:
        return bad_assertions_failure

    return structure.TestCase(
        variant=variant,
        input_overrides=overrides,
        current_resource=current_resource,
        resource_overlay=overlay,
        assertion=assertion,
        label=label,
        skip=spec.get("skip", False),
    )


def _check_for_resource_template_ref(
    function_under_test, base_inputs, test_cases: Sequence[structure.TestCase]
) -> None | Sequence[registry.Resource[ResourceTemplate]]:
    if not is_unwrapped_ok(function_under_test):
        return None

    if not isinstance(function_under_test, ResourceFunction):
        return None

    if not isinstance(
        function_under_test.crud_config.resource_template, ResourceTemplateRef
    ):
        return None

    if not function_under_test.crud_config.resource_template.name:
        return None

    template_name = function_under_test.crud_config.resource_template.name
    name_args = extract_argument_structure(template_name.ast)

    # There are no inputs used, just compute the value.
    if not name_args:
        match evaluate(expression=template_name, inputs={}, location="templateName"):
            case celtypes.StringType() as name:
                return (registry.Resource(resource_type=ResourceTemplate, name=name),)
            case _:
                return None

    templates_used = set()
    for test_case in test_cases:
        if base_inputs and test_case.input_overrides:
            inputs = _overlay(base_inputs, test_case.input_overrides)
            if isinstance(inputs, celpy.CELEvalError):
                continue
        elif base_inputs:
            inputs = base_inputs
        elif test_case.input_overrides:
            inputs = test_case.input_overrides
        else:
            inputs = celtypes.MapType()

        if function_under_test.local_values:
            locals = function_under_test.local_values.evaluate(
                {
                    "inputs": inputs,
                }
            )
        else:
            locals = celtypes.MapType()

        match evaluate(
            expression=template_name,
            inputs={
                "locals": locals,
                "inputs": inputs,
            },
            location="templateName",
        ):
            case celtypes.StringType() as name:
                templates_used.add(name)
            case _:
                continue

    return [
        registry.Resource(resource_type=ResourceTemplate, name=name)
        for name in templates_used
    ]
