from typing import Any

from koreo import registry
from koreo import result

from koreo.function.structure import Function
from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow

FunctionResource = (
    registry.Resource[ResourceFunction]
    | registry.Resource[ValueFunction]
    | registry.Resource[Function]
)


def function_or_workflow_to_resource(
    spec: Any, location_base: str | None = None
) -> FunctionResource | registry.Resource[Workflow] | None | result.PermFail:
    if not spec:
        return None

    if not isinstance(spec, dict):
        return result.PermFail(
            message=(
                "Expected object with a `functionRef` or `workflowRef`. "
                f"received {type(spec)}"
            ),
            location=location_base,
        )

    if resource := function_ref_spec_to_resource(spec=spec.get("functionRef")):
        return resource

    if resource := workflow_ref_spec_to_resource(spec=spec.get("workflowRef")):
        return resource

    return None


def function_ref_spec_to_resource(
    spec: Any, location_base: str | None = None
) -> FunctionResource | None | result.PermFail:
    if not spec:
        return None

    location = f"{location_base}:functionRef" if location_base else "functionRef"

    if not isinstance(spec, dict):
        return result.PermFail(
            message=(
                "Failed to process functionRef, expected object with kind and name. "
                f"received {type(spec)}"
            ),
            location=location,
        )

    kind = spec.get("kind")
    if not kind:
        return result.PermFail(
            message="functionRef is missing `kind`, kind and name are required.",
            location=location,
        )

    name = spec.get("name")
    if not name:
        return result.PermFail(
            message="functionRef is missing `name`, kind and name are required.",
            location=location,
        )

    if kind == "ValueFunction":
        return registry.Resource(resource_type=ValueFunction, name=name)
    elif kind == "ResourceFunction":
        return registry.Resource(resource_type=ResourceFunction, name=name)
    elif kind == "Function":
        return registry.Resource(resource_type=Function, name=name)

    return result.PermFail(
        message=f"Invalid `kind` ({kind}) in functionRef, kind must be one of "
        "`ResourceFunction`, `ValueFunction`, or `Function`.",
        location=location,
    )


def workflow_ref_spec_to_resource(
    spec: Any, location_base: str | None = None
) -> registry.Resource[Workflow] | None | result.PermFail:
    if not spec:
        return None

    location = f"{location_base}:workflowRef" if location_base else "workflowRef"

    if not isinstance(spec, dict):
        return result.PermFail(
            message=(
                "Failed to process workflowRef, expected object with name. "
                f"received {type(spec)}"
            ),
            location=location,
        )

    name = spec.get("name")
    if not name:
        return result.PermFail(
            message="workflowRef is missing `name`, name is required.",
            location=location,
        )

    return registry.Resource(resource_type=Workflow, name=name)
