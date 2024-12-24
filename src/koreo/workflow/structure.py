from __future__ import annotations
from typing import NamedTuple, Sequence

import celpy

from koreo.result import Outcome, ErrorOutcome

from koreo.value_function.structure import ValueFunction
from koreo.function.structure import Function


class ConfigCRDRef(NamedTuple):
    api_group: str
    version: str
    kind: str


class MappedInput(NamedTuple):
    source_iterator: celpy.Runner
    input_key: str


class StepConditionSpec(NamedTuple):
    type_: str
    name: str


class ConfigStep(NamedTuple):
    label: str
    logic: ValueFunction | Function | Workflow | ErrorOutcome

    inputs: celpy.Runner | None

    condition: StepConditionSpec | None


class Step(NamedTuple):
    label: str
    logic: ValueFunction | Function | Workflow | ErrorOutcome

    mapped_input: MappedInput | None
    inputs: celpy.Runner | None

    dynamic_input_keys: Sequence[str]

    condition: StepConditionSpec | None


class ErrorStep(NamedTuple):
    label: str
    outcome: Outcome

    condition: StepConditionSpec | None


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
