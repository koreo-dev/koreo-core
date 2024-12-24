from typing import Sequence
import logging

logger = logging.getLogger("koreo.valuefunction.prepare")


from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.result import PermFail, UnwrappedOutcome

from . import structure

import celpy

# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("Environment").setLevel(logging.WARNING)
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)


async def prepare_value_function(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.ValueFunction, None]]:
    celpy.CompiledRunner
    # NOTE: We can try `celpy.Environment(runner_class=celpy.CompiledRunner)`
    # We need to do a safety check to ensure there are no escapes / injections.
    logger.info(f"Prepare function {cache_key}")

    if not spec:
        return PermFail(
            message=f"Missing `spec` for Function '{cache_key}'.",
            location=_location(cache_key, "spec"),
        )

    if not isinstance(spec, dict):
        # This is needed since spec can technically be a non-dict at _runtime_.
        return PermFail(
            message=f"Malformed `spec` for Function '{cache_key}'.",
            location=_location(cache_key, "spec"),
        )

    env = celpy.Environment(annotations=koreo_function_annotations)

    spec_constants = spec.get("constants")
    match spec_constants:
        case None:
            constants = celpy.json_to_cel({})
        case dict():
            constants = celpy.json_to_cel(spec_constants)
        case _:
            return PermFail(
                message=f"Malformed `spec.constants` for Function '{cache_key}'.",
                location=_location(cache_key, "constants"),
            )

    used_vars = set[str]()

    match _predicate_extractor(cel_env=env, predicate_spec=spec.get("validators")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "validators")
            )
        case None:
            validators = None
        case celpy.Runner() as validators:
            used_vars.update(extract_argument_structure(validators.ast))

    match _prepare_return(cel_env=env, return_spec=spec.get("return")):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=f"prepare:Function:{cache_key}.return",
            )
        case None:
            return_value = None
        case celpy.Runner() as return_value:
            used_vars.update(extract_argument_structure(return_value.ast))

    return (
        structure.ValueFunction(
            validators=validators,
            constants=constants,
            return_value=return_value,
            dynamic_input_keys=used_vars,
        ),
        None,
    )


def _location(cache_key: str, extra: str | None = None) -> str:
    base = f"prepare:ValueFunction:{cache_key}"
    if not extra:
        return base

    return f"{base}:{extra}"


def _predicate_extractor(
    cel_env: celpy.Environment,
    predicate_spec: Sequence[dict] | None,
) -> celpy.Runner | None | PermFail:
    if not predicate_spec:
        return None

    if not isinstance(predicate_spec, (list, tuple)):
        return PermFail(message="Malformed `validators`, expected a list")

    predicates = encode_cel(predicate_spec)
    validators = f"{predicates}.filter(predicate, predicate.assert)"

    try:
        program = cel_env.program(
            cel_env.compile(validators), functions=koreo_cel_functions
        )
    except celpy.CELParseError as err:
        return PermFail(
            message=f"Parsing error at line {err.line}, column {err.column}. '{err}' in '{predicate_spec}'",
        )

    program.logger.setLevel(logging.WARNING)
    return program


def _prepare_return(
    cel_env: celpy.Environment, return_spec: dict | None
) -> celpy.Runner | None | PermFail:
    if not return_spec:
        return None

    if not isinstance(return_spec, dict):
        return PermFail(message="Malformed `return`, expected a mapping")

    try:
        return_value = cel_env.program(
            cel_env.compile(encode_cel(return_spec)), functions=koreo_cel_functions
        )
    except celpy.CELParseError as err:
        return PermFail(
            message=f"Parsing error at line {err.line}, column {err.column}. '{err}' in '{return_spec}'",
        )

    return_value.logger.setLevel(logging.WARNING)

    return return_value
