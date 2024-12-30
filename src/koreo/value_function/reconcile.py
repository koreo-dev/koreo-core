import celpy
from celpy import celtypes


from koreo.cel.evaluation import evaluate, evaluate_predicates
from koreo.result import PermFail, UnwrappedOutcome

from .structure import ValueFunction


async def reconcile_value_function(
    location: str,
    function: ValueFunction,
    inputs: celtypes.Value,
) -> UnwrappedOutcome[celtypes.Value]:
    full_inputs: dict[str, celtypes.Value] = {
        "inputs": inputs,
    }

    if validator_error := evaluate_predicates(
        predicates=function.validators,
        inputs=full_inputs,
        location="spec.validators",
    ):
        return validator_error

    # If there's no return_value, just bail out. No point in extra work.
    if not function.return_value:
        return celpy.json_to_cel(None)

    match evaluate(
        expression=function.local_values, inputs=full_inputs, location="spec.locals"
    ):
        case PermFail(message=message, location=locals_location):
            return PermFail(
                message=message,
                location=locals_location if locals_location else f"{location}.locals",
            )
        case celtypes.MapType() as local_values:
            full_inputs["locals"] = local_values
        case None:
            full_inputs["locals"] = celtypes.MapType({})
        case bad_type:
            # Due to validation within `prepare`, this should never happen.
            return PermFail(
                message=f"Invalid `locals` expression type ({type(bad_type)})",
                location=f"{location}.locals",
            )

    return evaluate(
        expression=function.return_value, inputs=full_inputs, location="spec.return"
    )
