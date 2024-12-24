from typing import Any
import re

from celpy import celtypes

CEL_PREFIX = "="


def convert_bools(
    cel_object: celtypes.Value,
) -> celtypes.Value | list[Any] | dict[Any, Any] | bool:
    """Recursive walk through the CEL object, replacing BoolType with native bool instances.
    This lets the :py:mod:`json` module correctly represent the obects
    with JSON ``true`` and ``false``.

    This will also replace ListType and MapType with native ``list`` and ``dict``.
    All other CEL objects will be left intact. This creates an intermediate hybrid
    beast that's not quite a :py:class:`celtypes.Value` because a few things have been replaced.
    """
    if isinstance(cel_object, celtypes.BoolType):
        return True if cel_object else False
    elif isinstance(cel_object, (celtypes.ListType, list)):
        return [convert_bools(item) for item in cel_object]
    elif isinstance(cel_object, (celtypes.MapType, dict)):
        return {
            convert_bools(key): convert_bools(value)
            for key, value in cel_object.items()
        }
    else:
        return cel_object


def encode_cel(value):
    if isinstance(value, dict):
        return f"{{{ ",".join(
            f'"{f"{key}".replace('"', '\"')}":{encode_cel(value)}'
            for key, value in value.items()
        ) }}}"

    if isinstance(value, list):
        return f"[{ ",".join(encode_cel(item) for item in value) }]"

    if isinstance(value, bool):
        return "true" if value else "false"

    if value is None:
        return "null"

    if _encode_plain(value):
        return f"{value}"

    if not value:
        return '""'

    if not value.startswith(CEL_PREFIX):
        return f'"{ value.replace('"', '\"') }"'  # fmt: skip

    return value.lstrip(CEL_PREFIX)


def encode_cel_template(template_spec: dict):
    return f"{{{','.join([f'"{field}": {expression}'
     for field, expression in _encode_template_dict("", template_spec)
     ])}}}"


QUOTED_NAME = re.compile(".*[^a-zA-Z0-9-_]+.*")


def _encode_template_dict(base: str, template_spec: dict):
    output: list[tuple[str, Any]] = []

    for field, expression in template_spec.items():
        if not isinstance(field, str):
            field = f"{field}"
        safe_field = field.replace('"', '\"')  # fmt: skip

        field_name = safe_field
        if base:
            if QUOTED_NAME.match(safe_field):
                field_name = f"{base}['{safe_field.replace("'", "\'")}']"  # fmt: skip
            else:
                field_name = f"{base}.{safe_field}"

        if isinstance(expression, dict):
            output.extend(_encode_template_dict(field_name, expression))
        else:
            output.append((field_name, encode_cel(expression)))

    return output


def _encode_plain(maybe_number) -> bool:
    if not isinstance(maybe_number, str):
        return True

    try:
        int(maybe_number)
        return True
    except ValueError:
        pass

    try:
        float(maybe_number)
        return True
    except ValueError:
        pass

    return False
