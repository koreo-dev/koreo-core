from typing import Any
import base64

from celpy import celtypes

CEL_PREFIX = "="

ConvertedType = (
    list["ConvertedType"]
    | dict["ConvertedType", "ConvertedType"]
    | str
    | int
    | float
    | bool
    | None
)


def convert_bools(
    cel_object: celtypes.Value,
) -> ConvertedType:
    """Recursive walk through the CEL object, replacing celtypes with native
    types. This lets the :py:mod:`json` module correctly represent the obects
    and allows Python code to treat these as normal objects.
    """
    match cel_object:
        case celtypes.BoolType():
            return True if cel_object else False

        case celtypes.StringType() | celtypes.TimestampType() | celtypes.DurationType():
            return str(cel_object)

        case celtypes.IntType() | celtypes.UintType():
            return int(cel_object)

        case celtypes.DoubleType():
            return float(cel_object)

        case celtypes.BytesType():
            return base64.b64encode(cel_object).decode("ASCII")

        case celtypes.ListType() | list() | tuple():
            return [convert_bools(item) for item in cel_object]

        case celtypes.MapType() | dict():
            return {
                convert_bools(key): convert_bools(value)
                for key, value in cel_object.items()
            }

        case celtypes.NullType():
            return None

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
