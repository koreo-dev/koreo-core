from typing import Any
import logging

import celpy

from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions
from koreo.result import PermFail


def prepare_map_expression(
    cel_env: celpy.Environment, spec: Any | None, name: str | None = None
) -> None | celpy.Runner | PermFail:
    if not spec:
        return None

    message_name = f"`{name}`" if name else "expression map"

    if not isinstance(spec, dict):
        return PermFail(message=f"Malformed {message_name}, expected a mapping")

    try:
        encoded = encode_cel(spec)
    except Exception as err:
        return PermFail(
            message=f"Structural error in {message_name}, while building expression '{err}'.",
        )

    try:
        locals_value = cel_env.program(
            cel_env.compile(encoded), functions=koreo_cel_functions
        )
    except celpy.CELParseError as err:
        return PermFail(
            message=f"Parsing error at line {err.line}, column {err.column}. '{err}' in {message_name} ('{encoded}')",
        )

    locals_value.logger.setLevel(logging.WARNING)

    return locals_value
