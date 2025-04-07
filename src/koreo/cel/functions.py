import copy
import json
import base64

from celpy import celtypes
import celpy


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


def _group_ref(
    source: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:
    reference = celtypes.MapType()

    api_group_key = celtypes.StringType("apiGroup")
    api_group = source.get(api_group_key)
    if api_group:
        reference[api_group_key] = api_group
    else:
        api_version_key = celtypes.StringType("apiVersion")
        api_version = source.get(api_version_key)
        if api_version and isinstance(api_version, celtypes.StringType):
            parts = api_version.split("/", 1)
            api_group = parts[0]
            reference[api_group_key] = celtypes.StringType(api_group)

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


def _kindless_ref(
    source: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:
    reference = celtypes.MapType()

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
    return _deep_overlay(resource, overlay)


def _deep_overlay(
    resource: celtypes.MapType,
    overlay: celtypes.MapType,
) -> celtypes.MapType | celpy.CELEvalError:
    resource = copy.deepcopy(resource)

    for field, overlay_value in overlay.items():
        if field in resource:
            resource_value = resource[field]
            if isinstance(overlay_value, celtypes.MapType) and isinstance(
                resource_value, celtypes.MapType
            ):
                updated_value = _deep_overlay(resource_value, overlay_value)

                if isinstance(updated_value, celpy.CELEvalError):
                    return updated_value

                resource[field] = updated_value

                continue

        resource[field] = overlay_value

    return resource


def _flatten(resource: celtypes.ListType) -> celtypes.ListType | celpy.CELEvalError:
    if not resource:
        return celtypes.ListType()

    merged = celtypes.ListType()

    for nested in resource:
        merged.extend(nested)

    return merged


def _lower(string: celtypes.StringType) -> celtypes.StringType | celpy.CELEvalError:
    return celtypes.StringType(string.lower())


def _strip(string: celtypes.StringType, on: celtypes.StringType) -> celtypes.StringType:
    return celtypes.StringType(string.strip(on))


def _rstrip(
    string: celtypes.StringType, on: celtypes.StringType
) -> celtypes.StringType:
    return celtypes.StringType(string.rstrip(on))


def _split(
    string: celtypes.StringType, on: celtypes.StringType
) -> celtypes.ListType | celpy.CELEvalError:
    if not on:
        return celpy.CELEvalError(f"split separator may not be empty")

    return celtypes.ListType(celtypes.StringType(part) for part in string.split(on))


def _split_first(
    string: celtypes.StringType, on: celtypes.StringType
) -> celtypes.StringType | celpy.CELEvalError:
    if not on:
        return celpy.CELEvalError(f"split separator may not be empty")

    if not string:
        return celtypes.StringType("")

    return celtypes.StringType(string.split(on, 1)[0])


def _split_last(
    string: celtypes.StringType, on: celtypes.StringType
) -> celtypes.StringType | celpy.CELEvalError:
    if not on:
        return celpy.CELEvalError(f"split separator may not be empty")

    if not string:
        return celtypes.StringType("")

    return celtypes.StringType(string.rsplit(on, 1)[-1])


def _split_index(
    string: celtypes.StringType, on: celtypes.StringType, index: celtypes.IntType
) -> celtypes.StringType | celpy.CELEvalError:
    if not on:
        return celpy.CELEvalError(f"split separator may not be empty")

    if not string:
        return celtypes.StringType("")

    try:
        split = string.split(on)[index]
    except IndexError:
        return celpy.CELEvalError(f"index out of bounds on split")

    return celtypes.StringType(split)


def _to_json(value: celtypes.Value) -> celtypes.StringType | celpy.CELEvalError:
    try:
        return celtypes.StringType(json.dumps(value, sort_keys=True))
    except Exception as err:
        return celpy.CELEvalError(f"JSON Encoding error {err}")


def _from_json(value: celtypes.StringType) -> celtypes.Value | celpy.CELEvalError:
    try:
        return celpy.json_to_cel(json.loads(value))
    except Exception as err:
        return celpy.CELEvalError(f"JSON Decoding error {err}")


def _b64encode(value: celtypes.Value) -> celtypes.StringType | celpy.CELEvalError:
    try:
        return celtypes.StringType(base64.b64encode(value.encode()).decode())
    except Exception as err:
        return celpy.CELEvalError(f"Base64 Encoding error {err}")


def _b64decode(value: celtypes.Value) -> celtypes.StringType | celpy.CELEvalError:
    try:
        return celtypes.StringType(base64.b64decode(value).decode("utf-8"))
    except Exception as err:
        return celpy.CELEvalError(f"Base64 Encoding error {err}")


def _replace(
    value: celtypes.StringType, old: celtypes.StringType, new: celtypes.StringType
) -> celtypes.StringType | celpy.CELEvalError:
    try:
        return celtypes.StringType(value.replace(old, new))
    except Exception as err:
        return celpy.CELEvalError(f"Replace error {err}")


koreo_function_annotations: dict[str, celpy.Annotation] = {
    "to_ref": celtypes.FunctionType,
    "self_ref": celtypes.FunctionType,
    "group_ref": celtypes.FunctionType,
    "kindless_ref": celtypes.FunctionType,
    "config_connect_ready": celtypes.FunctionType,
    "overlay": celtypes.FunctionType,
    "lower": celtypes.FunctionType,
    "flatten": celtypes.FunctionType,
    "split": celtypes.FunctionType,
    "split_first": celtypes.FunctionType,
    "split_last": celtypes.FunctionType,
    "split_index": celtypes.FunctionType,
    "strip": celtypes.FunctionType,
    "rstrip": celtypes.FunctionType,
    "to_json": celtypes.FunctionType,
    "from_json": celtypes.FunctionType,
    "b64encode": celtypes.FunctionType,
    "b64decode": celtypes.FunctionType,
    "replace": celtypes.FunctionType,
}

koreo_cel_functions: dict[str, celpy.CELFunction] = {
    "to_ref": _to_ref,
    "self_ref": _self_ref,
    "group_ref": _group_ref,
    "kindless_ref": _kindless_ref,
    "config_connect_ready": _config_connect_ready,
    "overlay": _overlay,
    "lower": _lower,
    "flatten": _flatten,
    "split": _split,
    "split_first": _split_first,
    "split_last": _split_last,
    "split_index": _split_index,
    "strip": _strip,
    "rstrip": _rstrip,
    "to_json": _to_json,
    "from_json": _from_json,
    "b64encode": _b64encode,
    "b64decode": _b64decode,
    "replace": _replace,
}
