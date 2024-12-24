from __future__ import annotations

from functools import reduce
from typing import Any, NoReturn, TypeIs, TypeVar, Iterable

import kopf


class DepSkip:
    """This is internal for skipping due to dependency fail."""

    message: str | None
    location: str | None

    def __init__(self, message: str | None = None, location: str | None = None):
        self.message = message
        self.location = location

    def combine(self, other: Outcome):
        return other

    def __str__(self) -> str:
        if self.message:
            return f"Dependency Skip (message={self.message})"

        return "Dependency Violation Skip"


class Skip:
    """Indicates that this was intentially skipped."""

    message: str | None
    location: str | None

    def __init__(self, message: str | None = None, location: str | None = None):
        self.message = message
        self.location = location

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip,)):
            return self

        return other

    def __str__(self) -> str:
        if self.message:
            return f"Skip (message={self.message})"

        return "User Skip"


class Ok[T]:
    """Indicates success and `self.data` contains a value of type `T`."""

    data: T
    location: str | None

    def __init__(self, data: T, location: str | None = None):
        self.data = data
        self.location = location

    def __str__(self) -> str:
        return f"Ok(data={self.data})"

    def combine(self, other: Outcome):
        if isinstance(other, (DepSkip, Skip)):
            return self

        if not isinstance(other, Ok):
            return other

        data = []
        if isinstance(self.data, list):
            data.extend(self.data)
        elif self.data is not None:
            data.append(self.data)

        if isinstance(other.data, list):
            data.extend(other.data)
        elif other.data is not None:
            data.append(other.data)

        location = []
        if self.location:
            location.append(self.location)

        if other.location:
            location.append(other.location)

        return Ok(data=data, location=", ".join(location))


class Retry:
    """Retry reconciliation after `self.delay` seconds."""

    message: str | None
    delay: int
    location: str | None

    def __init__(
        self, delay: int = 60, message: str | None = None, location: str | None = None
    ):
        self.message = message
        self.delay = delay
        self.location = location

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

        location = []
        if self.location:
            location.append(self.location)

        if other.location:
            location.append(other.location)

        return Retry(
            message=", ".join(message),
            delay=max(self.delay, other.delay),
            location=", ".join(location),
        )


class PermFail:
    """An error indicating retries should not be attempted."""

    message: str | None
    location: str | None

    def __init__(self, message: str | None = None, location: str | None = None):
        self.message = message
        self.location = location

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

        location = []
        if self.location:
            location.append(self.location)

        if other.location:
            location.append(other.location)

        return PermFail(message=", ".join(message), location=", ".join(location))


OkT = TypeVar("OkT")

ErrorOutcome = Retry | PermFail
SkipOutcome = DepSkip | Skip
NonOkOutcome = SkipOutcome | ErrorOutcome
Outcome = NonOkOutcome | Ok[OkT]
UnwrappedOutcome = NonOkOutcome | OkT

Outcomes = list[Outcome[OkT]]


def combine(outcomes: Iterable[Outcome]):
    if not outcomes:
        return Skip()

    return reduce(lambda acc, outcome: acc.combine(outcome), outcomes)


def unwrapped_combine(outcomes: Iterable[UnwrappedOutcome]) -> UnwrappedOutcome:
    if not outcomes:
        return Skip()

    outcome = reduce(
        lambda acc, outcome: acc.combine(
            Ok(outcome) if is_unwrapped_ok(outcome) else outcome
        ),
        outcomes,
        DepSkip(),  # The lowest priority in a combine flow
    )

    if is_ok(outcome):
        return outcome.data

    return outcome


def is_ok[T](candidate: Outcome[T]) -> TypeIs[Ok[T]]:
    if isinstance(candidate, Ok):
        return True

    return False


def is_unwrapped_ok[T](candidate: UnwrappedOutcome[T]) -> TypeIs[T]:
    if is_error(candidate):
        return False

    if is_skip(candidate):
        return False

    return True


def is_not_ok(candidate: Outcome) -> TypeIs[NonOkOutcome]:
    return not is_ok(candidate=candidate)


def is_error(candidate: Any) -> TypeIs[ErrorOutcome]:
    if isinstance(candidate, (Retry, PermFail)):
        return True

    return False


def is_skip(candidate: Any) -> TypeIs[SkipOutcome]:
    if isinstance(candidate, (DepSkip, Skip)):
        return True

    return False


def is_not_error(candidate: Outcome) -> TypeIs[Ok | Skip | DepSkip]:
    return not is_error(candidate=candidate)


def raise_for_error(error: Retry | PermFail) -> NoReturn:
    if isinstance(error, Retry):
        raise kopf.TemporaryError(error.message, delay=error.delay)

    raise kopf.PermanentError(error.message)
