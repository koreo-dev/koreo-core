import celpy
from celpy import celtypes

from koreo.cache import get_resource_from_cache
from koreo.result import PermFail, UnwrappedOutcome


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

    expected_resource_spec = spec.get("expectedResource", {})
    expected_resource = celpy.json_to_cel(expected_resource_spec)

    if expected_resource and not isinstance(expected_resource, celtypes.MapType):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.expectedResource` must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    parent = celpy.json_to_cel(spec.get("parent", {}))
    if parent and not isinstance(parent, celtypes.MapType):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.parent` ('{parent}') must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    inputs = celpy.json_to_cel(spec.get("inputs", {}))
    if inputs and not isinstance(inputs, celtypes.MapType):
        return PermFail(
            message=f"FunctionTest '{cache_key}' `spec.inputs` ('{inputs}') must be an object.",
            location=f"prepare:FunctionTest:{cache_key}",
        )

    index_test_function(test=cache_key, function=function_ref_name)

    return (
        structure.FunctionTest(
            function_under_test=function_under_test,
            parent=parent,
            inputs=inputs,
            expected_resource=expected_resource,
        ),
        None,
    )
