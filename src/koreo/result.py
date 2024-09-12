from __future__ import annotations

from functools import reduce
from typing import Any, NoReturn, TypeGuard, TypeVar

import kopf


class DepSkip:
    """This is internal for skipping due to dependency fail."""

    message: str | None

    def __init__(self, message: str | None = None):
        self.message = message

    def combine(self, other: Outcome):
        return other

    def __str__(self) -> str:
        if self.message:
            return f"Dependency Skip ({self.message})"

        return "Dependency Violation Skip"


class Skip:
    """Indicates that this was intentially skipped."""

    message: str | None

    def __init__(self, message: str | None = None):
        self.message = message

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip,)):
            return self

        return other

    def __str__(self) -> str:
        if self.message:
            return f"Skip ({self.message})"

        return "User Skip"


class Ok[T]:
    """Indicates success and `self.data` contains a value of type `T`."""

    data: T

    def __init__(self, data: T):
        self.data = data

    def __str__(self) -> str:
        return f"Ok({self.data})"

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip, Skip)):
            return self

        if isinstance(other, Ok):
            data = []
            if isinstance(self.data, list):
                data.extend(self.data)
            elif self.data is not None:
                data.append(self.data)

            if isinstance(other.data, list):
                data.extend(other.data)
            elif other.data is not None:
                data.append(other.data)

            return Ok(data=data)

        return other


class Retry:
    """Retry reconciliation after `self.delay` seconds."""

    message: str | None
    delay: int

    def __init__(self, delay: int = 60, message: str | None = None):
        self.message = message
        self.delay = delay

    def __str__(self) -> str:
        return f"Retry(delay={self.delay}, message={self.message})"

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip, Skip, Ok)):
            return self

        if isinstance(other, PermFail):
            return other

        message = []
        if self.message:
            message.append(self.message)

        if other.message:
            message.append(other.message)

        return Retry(
            message=", ".join(message),
            delay=max(self.delay, other.delay),
        )


class PermFail:
    """An error indicating retries should not be attempted."""

    message: str | None

    def __init__(self, message: str | None = None):
        self.message = message

    def __str__(self) -> str:
        return f"Permanent Failure (message={self.message})"

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip, Skip, Ok, Retry)):
            return self

        message = []
        if self.message:
            message.append(self.message)

        if other.message:
            message.append(other.message)

        return PermFail(message=", ".join(message))


OkT = TypeVar("OkT")

NonOkOutcome = DepSkip | Skip | Retry | PermFail
Outcome = NonOkOutcome | Ok[OkT]
UnwrappedOutcome = NonOkOutcome | OkT

Outcomes = list[Outcome[OkT]]


def combine(outcomes: Outcomes):
    if not outcomes:
        return Skip()

    return reduce(lambda acc, outcome: acc.combine(outcome), outcomes)


def is_ok[T](candidate: Outcome[T]) -> TypeGuard[Ok[T]]:
    if isinstance(candidate, Ok):
        return True

    return False


def is_not_ok(candidate: Outcome) -> TypeGuard[NonOkOutcome]:
    return not is_ok(candidate=candidate)


def is_error(candidate: Any) -> TypeGuard[Retry | PermFail]:
    if isinstance(candidate, (Retry, PermFail)):
        return True

    return False


def is_not_error(candidate: Outcome) -> TypeGuard[Ok | Skip | DepSkip]:
    return not is_error(candidate=candidate)


def raise_for_error(error: Retry | PermFail) -> NoReturn:
    if isinstance(error, Retry):
        raise kopf.TemporaryError(error.message, delay=error.delay)

    raise kopf.PermanentError(error.message)
