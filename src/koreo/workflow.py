from __future__ import annotations
from typing import Awaitable, Callable, Protocol
from collections.abc import Mapping

from .result import Outcome


class Workflow:
    steps: Mapping[str, ActionFn]
    completion: CompletionFn | None

    def __init__(
        self,
        steps: Mapping[str, ActionFn],
        completion: CompletionFn | None = None,
    ):
        self.steps = steps
        self.completion = completion


class ActionFn[T](Protocol):
    """
    `ActionFn` is used to specify the step to be run. To receive results from
    prior steps, name parameters the Workflow `step_id` and the Outcome of that
    `step_id` passed in. All arguments must be Outcome, or a sub-union of
    Outcomes.
    """

    __call__: Callable[..., Awaitable[Outcome[T]]]


class CompletionFn(Protocol):
    """
    `CompletionFn` is used to specify the final step to be run. It will receive
    the results from all prior steps as **results, the parameters names match
    the Workflow `step_id` and will be the Outcome of that `step_id`.
    """

    async def __call__(self, **results: Outcome) -> Outcome:
        raise NotImplemented("You must implement the function.")
