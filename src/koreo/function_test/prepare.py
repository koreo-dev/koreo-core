from typing import Sequence

import celpy
from celpy import celtypes

from koreo import ref_helpers
from koreo import registry
from koreo.cache import get_resource_from_cache
from koreo.result import DepSkip, PermFail, UnwrappedOutcome

from koreo.predicate_helpers import predicate_to_koreo_result
from koreo.value_function.structure import ValueFunction

from . import structure


async def prepare_function_test(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[tuple[structure.FunctionTest, Sequence[registry.Resource]]]:
    location = f"prepare:FunctionTest:{cache_key}"

    if not spec:
        return PermFail(
            message=f"Missing `spec` for FunctionTest '{cache_key}'.", location=location
        )

    match ref_helpers.function_ref_spec_to_resource(spec.get("functionRef")):
        case None:
            return PermFail(
                message=f"Missing `functionRef.name` for FunctionTest '{cache_key}'.",
                location=location,
            )
        case PermFail() as perm_fail:
            return perm_fail
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

    current_resource = spec.get("currentResource")
    if current_resource is not None and not isinstance(current_resource, dict):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.currentResource` must be an object.",
            location=location,
        )

    expected_resource = spec.get("expectedResource")
    if expected_resource is not None and not isinstance(expected_resource, dict):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.expectedResource` must be an object.",
            location=location,
        )

    if watched_function.resource_type == ValueFunction and (
        current_resource is not None or expected_resource is not None
    ):
        return PermFail(
            message=(
                f"`FunctionTest` for `{watched_function.resource_type.__name__}` "
                f"('{cache_key}') may not set `spec.currentResource` or `spec.expectedResource`."
            ),
            location=location,
        )

    inputs = celpy.json_to_cel(spec.get("inputs", {}))
    if inputs and not isinstance(inputs, celtypes.MapType):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.inputs` ('{inputs}') must be an object.",
            location=location,
        )

    expected_outcome_spec = spec.get("expectedOutcome")
    if expected_outcome_spec and isinstance(expected_outcome_spec, dict):
        expected_outcome_spec["assert"] = True
        expected_outcome = predicate_to_koreo_result(
            celpy.json_to_cel([expected_outcome_spec]), location=cache_key
        )
    else:
        expected_outcome = None

    expected_return = spec.get("expectedReturn")

    return (
        structure.FunctionTest(
            function_under_test=function_under_test,
            inputs=inputs,
            current_resource=current_resource,
            expected_resource=expected_resource,
            expected_outcome=expected_outcome,
            expected_return=expected_return,
        ),
        [watched_function],
    )
