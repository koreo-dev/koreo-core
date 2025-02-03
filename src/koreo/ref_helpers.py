from typing import Any

from koreo import registry
from koreo import result

from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow

FunctionResource = (
    registry.Resource[ResourceFunction] | registry.Resource[ValueFunction]
)


def function_or_workflow_to_resource(
    spec: Any, location: str
) -> FunctionResource | registry.Resource[Workflow] | None | result.PermFail:
    if not spec:
        return None

    if not isinstance(spec, dict):
        return result.PermFail(
            message=(
                "Expected object with a `functionRef` or `workflowRef`. "
                f"received {type(spec)}"
            ),
            location=location,
        )

    if resource := function_ref_spec_to_resource(
        spec=spec.get("functionRef"), location=f"{location}:functionRef"
    ):
        return resource

    if resource := workflow_ref_spec_to_resource(
        spec=spec.get("workflowRef"), location=f"{location}:workflowRef"
    ):
        return resource

    return None


def function_ref_spec_to_resource(
    spec: Any, location: str
) -> None | result.PermFail | FunctionResource:
    if spec is None:
        return None

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

    return result.PermFail(
        message=f"Invalid `kind` ({kind}) in functionRef, kind must be one of "
        "`ResourceFunction` or `ValueFunction`.",
        location=location,
    )


def workflow_ref_spec_to_resource(
    spec: Any, location: str
) -> None | registry.Resource[Workflow] | result.PermFail:
    if spec is None:
        return None

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
