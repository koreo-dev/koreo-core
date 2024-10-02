import copy
from datetime import UTC, datetime
from typing import TypedDict, NotRequired


class Condition(TypedDict):
    lastTransitionTime: NotRequired[str]
    lastUpdateTime: NotRequired[str]
    location: NotRequired[str | None]
    message: NotRequired[str | None]
    status: str
    reason: str
    type: str


Conditions = list[Condition]


def update_condition(conditions: Conditions, condition: Condition):
    conditions = copy.deepcopy(conditions)

    for index, candidate in enumerate(conditions):
        if candidate.get("type") == condition.get("type"):
            conditions[index] = _merge_conditions(candidate, condition)
            return conditions

    conditions.append(_new_condition(condition))
    return conditions


def _new_condition(new: Condition) -> Condition:
    event_time = datetime.now(UTC).isoformat()
    return Condition(
        lastUpdateTime=event_time,
        lastTransitionTime=event_time,
        type=new.get("type"),
        status=new.get("status"),
        reason=new.get("reason"),
        message=new.get("message"),
        location=new.get("location"),
    )


def _merge_conditions(base: Condition, updated: Condition) -> Condition:
    lastTransitionTime = base.get("lastTransitionTime")
    if base.get("status") != updated.get("status") or not lastTransitionTime:
        lastTransitionTime = datetime.now(UTC).isoformat()

    return Condition(
        type=base.get("type"),
        lastUpdateTime=datetime.now(UTC).isoformat(),
        lastTransitionTime=lastTransitionTime,
        status=updated.get("status"),
        reason=updated.get("reason"),
        message=updated.get("message"),
        location=updated.get("location"),
    )
