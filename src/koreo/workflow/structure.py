from __future__ import annotations
from typing import NamedTuple, Sequence

import celpy

from koreo.result import NonOkOutcome, Outcome

from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.structure import ValueFunction


class ConfigCRDRef(NamedTuple):
    api_group: str
    version: str
    kind: str


class StepConditionSpec(NamedTuple):
    type_: str
    name: str


class LogicSwitch(NamedTuple):
    switch_on: celpy.Runner
    logic_map: dict[str | int, ResourceFunction | ValueFunction | Workflow]
    default_logic: ResourceFunction | ValueFunction | Workflow | None

    dynamic_input_keys: set[str]


class Step(NamedTuple):
    label: str
    logic: ResourceFunction | ValueFunction | Workflow | LogicSwitch

    skip_if: celpy.Runner | None
    for_each: ForEach | None
    inputs: celpy.Runner | None

    condition: StepConditionSpec | None
    state: celpy.Runner | None

    dynamic_input_keys: set[str]


class ForEach(NamedTuple):
    source_iterator: celpy.Runner
    input_key: str
    condition: StepConditionSpec | None


class ErrorStep(NamedTuple):
    label: str
    outcome: NonOkOutcome

    condition: StepConditionSpec | None
    state: None = None


class Workflow(NamedTuple):
    name: str
    crd_ref: ConfigCRDRef | None

    steps_ready: Outcome
    steps: Sequence[Step | ErrorStep]

    dynamic_input_keys: set[str]
