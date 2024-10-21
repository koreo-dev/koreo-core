from __future__ import annotations
from typing import NamedTuple

import celpy

from koreo.result import Outcome
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


class Step(NamedTuple):
    label: str
    logic: Function | Workflow

    mapped_input: MappedInput | None
    inputs: celpy.Runner | None

    dynamic_input_keys: list[str]
    provided_input_keys: set[str]

    condition: StepConditionSpec | None


class ConditionSpec(NamedTuple):
    type_: str
    name: str
    step: str


class Status(NamedTuple):
    conditions: list[ConditionSpec]
    state: celpy.Runner | None


class Workflow(NamedTuple):
    crd_ref: ConfigCRDRef

    steps_ready: Outcome
    steps: list[Step]

    status: Status
