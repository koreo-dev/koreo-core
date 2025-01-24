from typing import Any

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
    match cel_object:
        case celtypes.BoolType():
            return True if cel_object else False

        case celtypes.ListType() | list() | tuple():
            return [convert_bools(item) for item in cel_object]

        case celtypes.MapType() | dict():
            return {
                convert_bools(key): convert_bools(value)
                for key, value in cel_object.items()
            }

        case _:
            return cel_object


def encode_cel(value: Any) -> str:
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
        if "\n" in value:
            return f'r"""{ value.replace('"', '\"') }"""'  # fmt: skip

        if '"' in value:
            return f'"""{ value.replace('"', r'\"') }"""'  # fmt: skip

        return f'"{value}"'  # fmt: skip

    return value.lstrip(CEL_PREFIX)


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
