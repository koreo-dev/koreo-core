import celpy
from celpy import celtypes


from koreo.cel.evaluation import evaluate, evaluate_predicates, evaluate_overlay
from koreo.result import PermFail, UnwrappedOutcome

from .structure import ValueFunction


async def reconcile_value_function(
    location: str,
    function: ValueFunction,
    inputs: celtypes.Value,
    value_base: celtypes.MapType | None = None,
) -> UnwrappedOutcome[celtypes.Value]:
    full_inputs: dict[str, celtypes.Value] = {
        "inputs": inputs,
    }

    # TODO: Need to decide if this is correct to do.
    if value_base:
        full_inputs = full_inputs | {celtypes.StringType("resource"): value_base}

    if precondition_error := evaluate_predicates(
        predicates=function.preconditions,
        inputs=full_inputs,
        location=f"{location}:spec.preconditions",
    ):
        return precondition_error

    # If there's no return_value, just bail out. No point in extra work.
    if not function.return_value:
        return celpy.json_to_cel(None)

    match evaluate(
        expression=function.local_values,
        inputs=full_inputs,
        location=f"{location}:spec.locals",
    ):
        case PermFail() as err:
            return err
        case celtypes.MapType() as local_values:
            full_inputs["locals"] = local_values
        case None:
            full_inputs["locals"] = celtypes.MapType({})
        case bad_type:
            # Due to validation within `prepare`, this should never happen.
            return PermFail(
                message=f"Invalid `locals` expression type ({type(bad_type)})",
                location=f"{location}:spec.locals",
            )

    if value_base is None:
        value_base = celtypes.MapType({})

    return evaluate_overlay(
        overlay=function.return_value,
        inputs=full_inputs,
        base=value_base,
        location=f"{location}:spec.return",
    )
