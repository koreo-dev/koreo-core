import copy
import logging

logger = logging.getLogger("koreo.cel.evaluation")

from celpy import celtypes
from celpy.celparser import tree_dump
import celpy

from koreo.predicate_helpers import predicate_to_koreo_result
from koreo.result import NonOkOutcome, PermFail

from koreo.cel.prepare import Index, Overlay


def evaluate(
    expression: celpy.Runner | None, inputs: dict[str, celtypes.Value], location: str
) -> None | celtypes.Value | PermFail:
    if not expression:
        return None

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


def evaluate_overlay(
    overlay: Overlay,
    inputs: dict[str, celtypes.Value],
    base: celtypes.MapType,
    location: str,
) -> PermFail | celtypes.MapType:
    if not isinstance(overlay.value_index, dict):
        return PermFail(
            f"Bad overlay structure for `{location}`, expected mapping",
            location=location,
        )

    combined_inputs = inputs | {celtypes.StringType("resource"): base}

    try:
        overlay_values = overlay.values.evaluate(combined_inputs)
        if eval_error := check_for_celevalerror(overlay_values, location):
            return eval_error

        if not isinstance(overlay_values, celtypes.ListType):
            return PermFail(
                f"Bad overlay structure for `{location}`, received {type(overlay_values)}.",
                location=location,
            )

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

    return _overlay_applier(base, index=overlay.value_index, values=overlay_values)


def _overlay_applier(
    base: celtypes.MapType, index: dict[str, Index], values: celtypes.ListType
) -> celtypes.MapType:
    overlaid = copy.deepcopy(base)
    for key, value_index in index.items():
        cel_key = celtypes.StringType(key)
        match value_index:
            case int():
                overlaid[cel_key] = values[value_index]
            case dict():
                match base.get(cel_key):
                    case dict() as sub_overlaid:
                        overlaid[cel_key] = _overlay_applier(
                            base=sub_overlaid,
                            index=value_index,
                            values=values,
                        )
                    case None | _:
                        overlaid[cel_key] = _overlay_applier(
                            base=celtypes.MapType(),
                            index=value_index,
                            values=values,
                        )
    return overlaid


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
