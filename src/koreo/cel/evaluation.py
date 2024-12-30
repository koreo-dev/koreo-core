import logging

logger = logging.getLogger("koreo.cel.evaluation")

from celpy import celtypes
from celpy.celparser import tree_dump
import celpy

from koreo.predicate_helpers import predicate_to_koreo_result
from koreo.result import NonOkOutcome, PermFail


def evaluate(
    expression: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
    location: str | None = None,
) -> None | celtypes.Value | PermFail:
    if not expression:
        return None

    if location:
        location = f"{location}"
    else:
        location = "expression"

    try:
        expression_value = expression.evaluate(inputs)

        if eval_errors := check_for_celevalerror(expression_value, location):
            return eval_errors

        return expression_value

    except celpy.CELEvalError as err:
        tree = tree_dump(err.tree) if err and err.tree else ""
        return PermFail(
            message=f"Error evaluating `{location}` (at {tree}) {err.args}",
            location=tree,
        )

    except:
        msg = f"Unknown failure evaluating `{location}`."
        logger.exception(msg)
        return PermFail(msg)


def evaluate_predicates(
    predicates: celpy.Runner | None, inputs: dict[str, celtypes.Value], location: str
) -> None | NonOkOutcome:
    if not predicates:
        return None

    if location:
        location = f"{location}"
    else:
        location = "validators"

    try:
        raw_result = predicates.evaluate(inputs)
        if eval_error := check_for_celevalerror(raw_result, location):
            return eval_error

        # This should be impossible, unless prepare validation was missed.
        if not isinstance(raw_result, celtypes.ListType):
            return PermFail(
                f"Bad structure for `{location}`, expected list of assertions but received {type(raw_result)}.",
                location=location,
            )

        return predicate_to_koreo_result(raw_result, location=location)

    except celpy.CELEvalError as err:
        tree = tree_dump(err.tree) if err and err.tree else ""
        return PermFail(
            message=f"Error evaluating `{location}` (at {tree}) {err.args}",
            location=tree,
        )
    except Exception as err:
        return PermFail(f"Error evaluating `{location}`: {err}", location=location)


def check_for_celevalerror(
    value: celtypes.Value | celpy.CELEvalError, location: str | None
) -> None | PermFail:
    match value:
        case celpy.CELEvalError(tree=error_tree):
            tree = tree_dump(error_tree) if error_tree else ""
            return PermFail(
                message=f"Error evaluating `{location}` (at {tree}) {value.args}",
                location=tree,
            )

        case celtypes.MapType() | dict():
            for key, subvalue in value.items():
                if eval_error := check_for_celevalerror(key, location):
                    return eval_error

                if eval_error := check_for_celevalerror(subvalue, location):
                    return eval_error

        case celtypes.ListType() | list() | tuple():
            for subvalue in value:
                if eval_error := check_for_celevalerror(subvalue, location):
                    return eval_error

    return None
