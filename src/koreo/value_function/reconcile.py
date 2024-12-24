import json
import logging

logger = logging.getLogger("koreo.function")

import celpy
from celpy import celtypes
from celpy.celparser import tree_dump


from koreo.result import DepSkip, NonOkOutcome, PermFail, Retry, Skip, UnwrappedOutcome

from .structure import ValueFunction


async def reconcile_value_function(
    location: str,
    function: ValueFunction,
    inputs: celtypes.Value,
) -> UnwrappedOutcome[celtypes.Value]:
    full_inputs: dict[str, celtypes.Value] = {
        "inputs": inputs,
        "constants": function.constants,
    }

    if validator_error := _run_validators(
        validators=function.validators, inputs=full_inputs, location=location
    ):
        return validator_error

    if not function.return_value:
        return celpy.json_to_cel(None)

    try:
        return_value = function.return_value.evaluate(full_inputs)

        if eval_errors := _check_for_celevalerror(return_value, "return value"):
            return eval_errors

        return return_value
    except celpy.CELEvalError as err:
        tree = tree_dump(err.tree) if err and err.tree else ""
        return PermFail(
            message=f"Error evaluating `return` (at {tree}) {err.args}",
            location=tree,
        )
    except:
        msg = "Unknown failure evaluating `return` value."
        logger.exception(msg)
        return PermFail(msg, location=location)


def _check_for_celevalerror(value: celtypes.Value, name: str) -> NonOkOutcome | None:
    if isinstance(value, celpy.CELEvalError):
        tree = tree_dump(value.tree) if value and value.tree else ""
        return PermFail(
            message=f"Error evaluating `{name}` (at {tree}) {value.args}",
            location=tree,
        )

    if isinstance(value, celtypes.MapType):
        for key, subvalue in value.items():
            if eval_error := _check_for_celevalerror(key, name):
                return eval_error

            if eval_error := _check_for_celevalerror(subvalue, name):
                return eval_error

    if isinstance(value, celtypes.ListType):
        for subvalue in value:
            if eval_error := _check_for_celevalerror(subvalue, name):
                return eval_error

    return None


def _run_validators(
    validators: celpy.Runner | None, inputs: dict[str, celtypes.Value], location: str
) -> NonOkOutcome | None:
    if not validators:
        return None

    try:
        raw_result = validators.evaluate(inputs)
        if eval_error := _check_for_celevalerror(raw_result, location):
            return eval_error

        return _validator_to_koreo_result(raw_result, location=location)

    except celpy.CELEvalError as err:
        tree = tree_dump(err.tree) if err and err.tree else ""
        return PermFail(
            message=f"Error evaluating `validators` (at {tree}) {err.args}",
            location=tree,
        )
    except Exception as err:
        return PermFail(
            f"Error evaluating `validators` for {location}. {err}", location=location
        )


def _validator_to_koreo_result(
    results: celtypes.Value, location: str
) -> NonOkOutcome | None:
    if not results:
        return None

    if not isinstance(results, celtypes.ListType):
        return PermFail(f"Malformed validators: {results}", location=location)

    for result in results:
        match result:
            case {"assert": _, "ok": {}}:
                return None

            case {"assert": _, "depSkip": {"message": message}}:
                return DepSkip(message=message, location=location)

            case {"assert": _, "skip": {"message": message}}:
                return Skip(message=message, location=location)

            case {"assert": _, "retry": {"message": message, "delay": delay}}:
                return Retry(
                    message=message,
                    delay=delay,
                    location=location,
                )

            case {"assert": _, "permFail": {"message": message}}:
                return PermFail(message=message, location=location)

            case _:
                return PermFail(
                    f"Unknown predicate type: {json.dumps(result)}", location=location
                )

    return None
