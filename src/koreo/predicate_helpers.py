from typing import Sequence
import json
import logging

from celpy import celtypes
import celpy

from koreo import result
from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions


def predicate_extractor(
    cel_env: celpy.Environment,
    predicate_spec: Sequence[dict] | None,
) -> None | celpy.Runner | result.PermFail:
    if not predicate_spec:
        return None

    if not isinstance(predicate_spec, (list, tuple)):
        return result.PermFail(message="Malformed conditions, expected a list")

    predicates = encode_cel(predicate_spec)
    conditions = f"{predicates}.filter(predicate, !predicate.assert)"

    try:
        program = cel_env.program(
            cel_env.compile(conditions), functions=koreo_cel_functions
        )
    except celpy.CELParseError as err:
        return result.PermFail(
            message=f"Parsing error at line {err.line}, column {err.column}. "
            f"'{err}' in '{predicates}'",
        )

    program.logger.setLevel(logging.WARNING)
    return program


def predicate_to_koreo_result(
    predicates: celtypes.ListType, location: str
) -> result.NonOkOutcome | None:
    if not predicates:
        return None

    for predicate in predicates:
        match predicate:
            case {"assert": _, "ok": {}}:
                return None

            case {"assert": _, "depSkip": {"message": message}}:
                return result.DepSkip(message=f"{message}", location=location)

            case {"assert": _, "skip": {"message": message}}:
                return result.Skip(message=f"{message}", location=location)

            case {"assert": _, "retry": {"message": message, "delay": delay}}:
                return result.Retry(
                    message=f"{message}",
                    delay=int(f"{delay}"),
                    location=location,
                )

            case {"assert": _, "permFail": {"message": message}}:
                return result.PermFail(message=f"{message}", location=location)

            case _:
                return result.PermFail(
                    f"Unknown predicate type: {json.dumps(predicate)}",
                    location=location,
                )

    return None
