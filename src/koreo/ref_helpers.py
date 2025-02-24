from typing import Any

from koreo import registry
from koreo import result

from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow

FunctionResource = (
    registry.Resource[ResourceFunction] | registry.Resource[ValueFunction]
)

LogicResource = (
    registry.Resource[ResourceFunction]
    | registry.Resource[ValueFunction]
    | registry.Resource[Workflow]
)


def function_ref_spec_to_resource(
    spec: Any, location: str
) -> None | FunctionResource | result.PermFail:
    if not spec:
        return None

    if not isinstance(spec, dict):
        return result.PermFail(
            message=(
                "Failed to process `functionRef`, expected object with `kind` and `name`. "
                f"received {type(spec)}"
            ),
            location=location,
        )

    logic_kind = spec.get("kind")
    logic_name = spec.get("name")

    if not logic_kind:
        return result.PermFail(message="Missing `functionRef.kind`.", location=location)

    if not logic_name:
        return result.PermFail(
            message=f"Missing `functionRef.name`.", location=location
        )

    # This is not using a "dict lookup" because of Python's deficient type
    # narrowing.
    match logic_kind:
        case "ValueFunction":
            return registry.Resource(resource_type=ValueFunction, name=logic_name)
        case "ResourceFunction":
            return registry.Resource(resource_type=ResourceFunction, name=logic_name)
        case _:
            return result.PermFail(
                message=f"Invalid `functionRef.kind` ({logic_kind}).", location=location
            )
