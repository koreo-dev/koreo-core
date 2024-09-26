from typing import Any, NamedTuple

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


class FunctionRef(NamedTuple):
    label: str
    function: Function

    mapped_input: MappedInput | None
    inputs: celpy.Runner | None
    dynamic_input_keys: list[str]


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
    steps: list[FunctionRef]

    status: Status
