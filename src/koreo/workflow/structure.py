from __future__ import annotations
from typing import NamedTuple, Sequence

import celpy

from koreo.result import NonOkOutcome, Outcome

from koreo.function.structure import Function
from koreo.resource_function.structure import ResourceFunction
from koreo.value_function.structure import ValueFunction


class ConfigCRDRef(NamedTuple):
    api_group: str
    version: str
    kind: str


class StepConditionSpec(NamedTuple):
    type_: str
    name: str


class ConfigStep(NamedTuple):
    label: str
    logic: ResourceFunction | ValueFunction | Function | Workflow

    inputs: celpy.Runner | None

    condition: StepConditionSpec | None
    state: celpy.Runner | None


class Step(NamedTuple):
    label: str
    logic: ResourceFunction | ValueFunction | Function | Workflow

    for_each: ForEach | None
    inputs: celpy.Runner | None

    condition: StepConditionSpec | None
    state: celpy.Runner | None

    dynamic_input_keys: Sequence[str]


class ForEach(NamedTuple):
    source_iterator: celpy.Runner
    input_key: str
    condition: StepConditionSpec | None


class ErrorStep(NamedTuple):
    label: str
    outcome: NonOkOutcome

    condition: StepConditionSpec | None
    state: None = None


class ConditionSpec(NamedTuple):
    type_: str
    name: str
    step: str


class Status(NamedTuple):
    conditions: Sequence[ConditionSpec]
    state: celpy.Runner | None


class Workflow(NamedTuple):
    crd_ref: ConfigCRDRef | None

    steps_ready: Outcome
    config_step: ConfigStep | ErrorStep | None
    steps: Sequence[Step | ErrorStep]

    status: Status
