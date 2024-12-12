import celpy
from celpy import celtypes

from koreo.cache import get_resource_from_cache
from koreo.result import PermFail, UnwrappedOutcome

from koreo.function.reconcile import _predicate_to_koreo_result

from . import structure
from .registry import index_test_function


async def prepare_function_test(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[tuple[structure.FunctionTest, None]]:
    if not spec:
        return PermFail(
            message=f"Missing `spec` for FunctionTest '{cache_key}'.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    function_ref_name = spec.get("functionRef", {}).get("name")
    if not function_ref_name:
        return PermFail(
            message=f"Missing `functionRef.name` for FunctionTest '{cache_key}'.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    function_under_test = get_resource_from_cache(structure.Function, function_ref_name)
    if not function_under_test:
        return PermFail(
            message=f"Function ({function_ref_name}) not found or not ready, FunctionTest '{cache_key}' must wait.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    current_resource = spec.get("currentResource")
    if current_resource is not None and not isinstance(current_resource, dict):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.currentResource` must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    expected_resource = spec.get("expectedResource")
    if expected_resource is not None and not isinstance(expected_resource, dict):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.expectedResource` must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    inputs = celpy.json_to_cel(spec.get("inputs", {}))
    if inputs and not isinstance(inputs, celtypes.MapType):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.inputs` ('{inputs}') must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    expected_outcome_spec = spec.get("expectedOutcome")
    if expected_outcome_spec:
        expected_outcome_spec["assert"] = True
        expected_outcome = _predicate_to_koreo_result(
            [expected_outcome_spec], location=cache_key
        )
    else:
        expected_outcome = None

    expected_ok_value = spec.get("expectedOkValue")

    index_test_function(test=cache_key, function=function_ref_name)

    return (
        structure.FunctionTest(
            function_under_test=function_under_test,
            inputs=inputs,
            current_resource=current_resource,
            expected_resource=expected_resource,
            expected_outcome=expected_outcome,
            expected_ok_value=expected_ok_value,
        ),
        None,
    )
