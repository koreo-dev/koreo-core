import logging

logger = logging.getLogger("koreo.valuefunction.prepare")

import celpy

from koreo import schema
from koreo.cel.functions import koreo_function_annotations
from koreo.cel.prepare import (
    Overlay,
    prepare_map_expression,
    prepare_overlay_expression,
)
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.predicate_helpers import predicate_extractor
from koreo.result import PermFail, UnwrappedOutcome

from . import structure

# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("Environment").setLevel(logging.WARNING)
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)


async def prepare_value_function(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.ValueFunction, None]]:
    logger.debug(f"Prepare ValueFunction:{cache_key}")

    if error := schema.validate(
        resource_type=structure.ValueFunction, spec=spec, validation_required=True
    ):
        return PermFail(
            error.message,
            location=_location(cache_key, "spec"),
        )

    env = celpy.Environment(annotations=koreo_function_annotations)

    used_vars = set[str]()

    match predicate_extractor(cel_env=env, predicate_spec=spec.get("preconditions")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.preconditions")
            )
        case None:
            preconditions = None
        case celpy.Runner() as preconditions:
            used_vars.update(extract_argument_structure(preconditions.ast))

    match prepare_map_expression(
        cel_env=env, spec=spec.get("locals"), location="spec.locals"
    ):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=_location(cache_key, "spec.locals"),
            )
        case None:
            local_values = None
        case celpy.Runner() as local_values:
            used_vars.update(extract_argument_structure(local_values.ast))

    match prepare_overlay_expression(
        cel_env=env, spec=spec.get("return"), location="spec.return"
    ):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=_location(cache_key, "spec.return"),
            )
        case None:
            return_value = None

        case Overlay(values=values) as return_value:
            used_vars.update(extract_argument_structure(values.ast))

    return (
        structure.ValueFunction(
            preconditions=preconditions,
            local_values=local_values,
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
