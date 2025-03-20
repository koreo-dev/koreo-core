from typing import NamedTuple, Sequence

from koreo.constants import KOREO_DIRECTIVE_KEYS


class ResourceMatch(NamedTuple):
    match: bool
    differences: Sequence[str]


def validate_match(
    target,
    actual,
    last_applied_value=None,
    compare_list_as_set: bool = False,
) -> ResourceMatch:
    """Compare the specified (`target`) state against the actual (`actual`)
    reosurce state. We compare all target fields and ignore anything extra so
    that defaults, controller set, or explicitly set values do not cause
    compare issues.
    """

    match (target, actual):
        # Objects need a special comparator
        case dict(), dict():
            return _validate_dict_match(target, actual, last_applied_value)
        case dict(), _:
            return ResourceMatch(
                match=False,
                differences=[f"<target:object, resource:{type(actual)}>"],
            )
        case _, dict():
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:object>"],
            )

        # Arrays need a special comparator
        case (list() | tuple(), list() | tuple()):
            if compare_list_as_set:
                # Sets must be simple, so if needed last_applied_value is
                # already the compare value.
                return _validate_set_match(target, actual)

            return _validate_list_match(target, actual, last_applied_value)

        case (list() | tuple(), _):
            return ResourceMatch(
                match=False,
                differences=[f"<target:array, resource:{type(actual)}>"],
            )
        case (_, list() | tuple()):
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:array>"],
            )

        # Bool needs a special comparator, due to Python's int truthiness rules
        case bool(), bool():
            if target == actual:
                return ResourceMatch(match=True, differences=())
            else:
                return ResourceMatch(
                    match=False,
                    differences=[f"<target:[{target}], resource:[{actual}]>"],
                )
        case bool(), _:
            return ResourceMatch(
                match=False,
                differences=[f"<target:bool, resource:{type(actual)}>"],
            )
        case _, bool():
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:bool>"],
            )

        case _, None if target:
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:null or missing>"],
            )

        case None, _ if actual:
            return ResourceMatch(
                match=False,
                differences=[f"<target:null, resource:{type(actual)}"],
            )

    # Hopefully anything else is a simple type.
    if target == actual:
        return ResourceMatch(match=True, differences=())
    else:
        return ResourceMatch(
            match=False,
            differences=[f"<target:[{target}], resource:[{actual}]>"],
        )


def _obj_to_key(obj: dict, fields: Sequence[int | str]) -> str:
    return "$".join(f"{obj.get(field)}".strip() for field in fields)


def _list_to_object(
    obj_list: Sequence[dict], key_fields: Sequence[int | str]
) -> dict[str, dict] | None:
    if not obj_list:
        return None

    return {_obj_to_key(obj, key_fields): obj for obj in obj_list}


def _validate_dict_match(
    target: dict, actual: dict, last_applied_value: dict | None = None
) -> ResourceMatch:
    target_keys = set(target.keys()) - KOREO_DIRECTIVE_KEYS

    compare_list_as_set_keys = {
        key for key in target.get("x-koreo-compare-as-set", ()) if key
    }

    compare_last_applied_keys = {
        key for key in target.get("x-koreo-compare-last-applied", ()) if key
    }

    compare_as_map = {
        key: [field_name for field_name in fields if field_name]
        for key, fields in target.get("x-koreo-compare-as-map", {}).items()
        if key
    }

    if not last_applied_value:
        last_applied_value = {}

    for target_key in target_keys:
        # TODO: At some point, this will appear outside metadata and eventually
        # cause a problem. Perhaps `_extract_last_applied` should instead
        # mutate the object for compare?
        if target_key == "ownerReferences":
            continue

        # NOTE: I'm not sure this is correct.
        if target_key not in last_applied_value:
            last_applied_value[target_key] = None

        if target_key in compare_last_applied_keys:
            compare_value = last_applied_value[target_key]

        else:
            if target_key not in actual:
                return ResourceMatch(
                    match=False, differences=[f"<missing '{target_key}' in resource>"]
                )

            compare_value = actual[target_key]

        if target_key in compare_as_map:
            map_keys = compare_as_map[target_key]
            key_match = validate_match(
                target=_list_to_object(target[target_key], map_keys),
                actual=_list_to_object(compare_value, map_keys),
                last_applied_value=_list_to_object(
                    last_applied_value[target_key], map_keys
                ),
            )
        else:
            key_match = validate_match(
                target=target[target_key],
                actual=compare_value,
                last_applied_value=last_applied_value[target_key],
                compare_list_as_set=(target_key in compare_list_as_set_keys),
            )

        if not key_match.match:
            differences = [f"'{target_key}'"]
            differences.extend(key_match.differences)
            return ResourceMatch(match=False, differences=differences)

    return ResourceMatch(match=True, differences=())


def _validate_list_match(
    target: list | tuple,
    actual: list | tuple,
    last_applied_value: list | tuple | None = None,
) -> ResourceMatch:
    if not target and not actual:
        return ResourceMatch(match=True, differences=())

    if target is None and actual:
        return ResourceMatch(match=False, differences=("<expected null array>",))

    if target and actual is None:
        return ResourceMatch(
            match=False, differences=("<unexpectedly found null array>",)
        )

    if len(target) != len(actual):
        return ResourceMatch(
            match=False,
            differences=[
                f"<length mismatch target:{len(target)}, actual:{len(actual)}"
            ],
        )

    if last_applied_value:
        last_applied_len = len(last_applied_value)
    else:
        last_applied_len = 0

    for idx, (target_value, actual_value) in enumerate(zip(target, actual)):
        if last_applied_value and idx < last_applied_len:
            last_applied_item_value = last_applied_value[idx]
        else:
            last_applied_item_value = None

        item_match = validate_match(
            target=target_value,
            actual=actual_value,
            last_applied_value=last_applied_item_value,
        )
        if not item_match.match:
            differences = [f"at index '{idx}'"]
            differences.extend(item_match.differences)
            return ResourceMatch(match=False, differences=differences)

    return ResourceMatch(match=True, differences=())


def _validate_set_match(target: list | tuple, actual: list | tuple) -> ResourceMatch:
    if not target and not actual:
        return ResourceMatch(match=True, differences=())

    if target is None and actual:
        return ResourceMatch(match=False, differences=("<expected null set>",))

    if target and actual is None:
        return ResourceMatch(
            match=False, differences=("<unexpectedly found null set>",)
        )

    try:
        target_set = set(target)
        actual_set = set(actual)
    except TypeError as err:
        if "dict" in f"{err}":
            return ResourceMatch(
                match=False,
                differences=(
                    f"Could not compare array-as-set, try: `x-koreo-compare-as-map`",
                ),
            )

        return ResourceMatch(
            match=False, differences=(f"Could not compare array-as-set ({err})",)
        )
    except Exception as err:
        return ResourceMatch(
            match=False, differences=(f"Could not compare array-as-set ({err})",)
        )

    missing_values = target_set - actual_set
    unexpected_values = actual_set - target_set

    if not (missing_values or unexpected_values):
        return ResourceMatch(match=True, differences=())

    for missing_value in missing_values:
        return ResourceMatch(match=False, differences=(f"<missing '{missing_value}'>",))

    for unexpected_value in unexpected_values:
        return ResourceMatch(
            match=False, differences=(f"<unexpectedly found '{unexpected_value}'>",)
        )

    # This is impossible
    return ResourceMatch(match=True, differences=())
