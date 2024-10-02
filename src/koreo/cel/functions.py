from typing import Any

from celpy import celtypes
import celpy

import jsonpath_ng

# NOTE: Annotations and Function List are at the bottom of the file. Sorry.


def _self_ref(
    resource: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:

    api_version_key = celtypes.StringType("apiVersion")
    api_version = resource.get(api_version_key)
    if not api_version:
        return celpy.CELEvalError(f"Missing `apiVersion`.")

    kind_key = celtypes.StringType("kind")
    kind = resource.get(kind_key)
    if not kind:
        return celpy.CELEvalError(f"Missing `kind`.")

    metadata_key = celtypes.StringType("metadata")
    metadata = resource.get(metadata_key)
    if not metadata:
        return celpy.CELEvalError(f"Missing `metadata`.")

    name_key = celtypes.StringType("name")
    name = metadata.get(name_key)
    if not name:
        return celpy.CELEvalError(f"Missing `metadata.name`.")

    namespace_key = celtypes.StringType("namespace")
    namespace = metadata.get(namespace_key)
    if not namespace:
        return celpy.CELEvalError(f"Missing `metadata.namespace`.")

    reference = celtypes.MapType(
        {
            api_version_key: api_version,
            kind_key: kind,
            name_key: name,
            namespace_key: namespace,
        }
    )

    return reference


def _to_ref(
    source: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:
    reference = celtypes.MapType()

    api_version_key = celtypes.StringType("apiVersion")
    api_version = source.get(api_version_key)
    if api_version:
        reference[api_version_key] = api_version

    kind_key = celtypes.StringType("kind")
    kind = source.get(kind_key)
    if kind:
        reference[kind_key] = kind

    if "external" in source:
        external_key = celtypes.StringType("external")
        external = source.get(external_key)
        if not external:
            return celpy.CELEvalError(f"`external` must contain a value.")

        reference[external_key] = external
        return reference

    if "name" not in source:
        return celpy.CELEvalError(
            f"`external` or `name` are required to build a reference."
        )

    name_key = celtypes.StringType("name")
    name = source.get(name_key)

    if not name:
        return celpy.CELEvalError(f"`name` must contain a value.")

    reference[name_key] = name

    namespace_key = celtypes.StringType("namespace")
    namespace = source.get(namespace_key)
    if namespace:
        reference[namespace_key] = namespace

    return reference


def _config_connect_ready(
    resource: celtypes.MapType,
) -> celtypes.BoolType | celpy.CELEvalError:
    if "status" not in resource:
        return celtypes.BoolType(False)

    status: celtypes.MapType = resource.get("status")

    if "conditions" not in status:
        return celtypes.BoolType(False)

    conditions: celtypes.ListType = status.get("conditions")

    ready_condition = None

    for condition in conditions:
        condition_type = condition.get("type")
        if condition_type != "Ready":
            continue

        # This is done to ensure there is only one 'type=Ready' I am unsure if
        # there is a valid case for multiple 'type=Ready' conditions existing.
        # If there is, we'll need to inspect it and decide how to handle it.
        if ready_condition is not None:
            return celtypes.BoolType(False)

        ready_condition = condition

    if not ready_condition:
        return celtypes.BoolType(False)

    reason = ready_condition.get("reason")

    if reason != "UpToDate":
        return celtypes.BoolType(False)

    condition_status = ready_condition.get("status")

    if condition_status != "True":
        return celtypes.BoolType(False)

    return celtypes.BoolType(True)


def _overlay(
    resource: celtypes.MapType,
    overlay: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:
    computed_overlay = __build_overlay_structure(overlay)

    for field_path, value in computed_overlay:
        try:
            path = jsonpath_ng.parse(field_path)
            path.update_or_create(resource, value)
        except Exception as err:
            return celpy.CELEvalError(f"Error applying template overlay ({err}).")

    return resource


def __build_overlay_structure(overlay: celtypes.MapType, base: str | None = None):
    output: list[tuple[str, Any]] = []

    for field, value in overlay.items():
        if not base:
            field_path = f"{field}"
        else:
            if base.endswith("labels") or base.endswith("annotations"):
                field_path = f"{base}['{field}']"
            else:
                field_path = f"{base}.{field}"

        if isinstance(value, celtypes.MapType):
            output.extend(__build_overlay_structure(value, base=field_path))
        else:
            output.append((field_path, value))

    return output


def _template_name(
    resource: celtypes.MapType, name: celtypes.StringType
) -> celtypes.StringType | celpy.CELEvalError:
    api_version_key = celtypes.StringType("apiVersion")
    api_version = resource.get(api_version_key)
    if not api_version:
        return celpy.CELEvalError(f"Missing `apiVersion`.")

    kind_key = celtypes.StringType("kind")
    kind = resource.get(kind_key)
    if not kind:
        return celpy.CELEvalError(f"Missing `kind`.")

    if not name:
        return celpy.CELEvalError(f"Missing `name`.")

    return celtypes.StringType(f"{kind}.{api_version}.{name}")


koreo_function_annotations: dict[str, celpy.Annotation] = {
    "to_ref": celtypes.FunctionType,
    "self_ref": celtypes.FunctionType,
    "config_connect_ready": celtypes.FunctionType,
    "overlay": celtypes.FunctionType,
    "template_name": celtypes.FunctionType,
}

koreo_cel_functions: dict[str, celpy.CELFunction] = {
    "to_ref": _to_ref,
    "self_ref": _self_ref,
    "config_connect_ready": _config_connect_ready,
    "overlay": _overlay,
    "template_name": _template_name,
}
