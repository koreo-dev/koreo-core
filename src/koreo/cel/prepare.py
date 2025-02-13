from typing import Any, NamedTuple
import logging

import celpy

from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions
from koreo.result import PermFail


def prepare_expression(
    cel_env: celpy.Environment, spec: Any | None, location: str
) -> None | celpy.Runner | PermFail:
    if not spec:
        return None

    try:
        encoded = encode_cel(spec)
    except Exception as err:
        return PermFail(
            message=f"Structural error in {location}, while building expression '{err}'.",
        )

    try:
        value = cel_env.program(cel_env.compile(encoded), functions=koreo_cel_functions)
    except celpy.CELParseError as err:
        return PermFail(
            message=f"Parsing error at line {err.line}, column {err.column}. '{err}' in {location} ('{encoded}')",
        )

    value.logger.setLevel(logging.WARNING)

    return value


def prepare_map_expression(
    cel_env: celpy.Environment, spec: Any | None, location: str
) -> None | celpy.Runner | PermFail:
    if not spec:
        return None

    if not isinstance(spec, dict):
        return PermFail(message=f"Malformed {location}, expected a mapping")

    return prepare_expression(cel_env=cel_env, spec=spec, location=location)


Index = dict[str, "Index"] | int


class Overlay(NamedTuple):
    value_index: Index
    values: celpy.Runner


def prepare_overlay_expression(
    cel_env: celpy.Environment, spec: Any | None, location: str
) -> None | Overlay | PermFail:
    if not spec:
        return None

    if not isinstance(spec, dict):
        return PermFail(message=f"Malformed {location}, expected a mapping")

    overlay_index, overlay_values = _overlay_indexer(spec=spec, base=0)

    match prepare_expression(cel_env=cel_env, spec=overlay_values, location=location):
        case None:
            return None
        case PermFail() as err:
            return err
        case celpy.Runner() as value_expression:
            return Overlay(value_index=overlay_index, values=value_expression)


def _overlay_indexer(spec: dict, base: int = 0) -> tuple[Index, list]:
    index = {}
    values = []
    for key, value in spec.items():
        match value:
            case dict() if value:
                index[key], key_values = _overlay_indexer(
                    value, base=len(values) + base
                )
                values.extend(key_values)
            case _:
                index[key] = len(values) + base
                values.append(value)

    return index, values
