from typing import Any, NamedTuple

import celpy

from koreo.result import Outcome
from koreo.function.structure import Function


class ConfigCRDRef(NamedTuple):
    api_group: str
    version: str
    kind: str


class FunctionRef(NamedTuple):
    label: str
    function: Function

    inputs: celpy.Runner | None
    dynamic_input_keys: list[str]

    static_inputs: dict[str, Any]


class Workflow(NamedTuple):
    crd_ref: ConfigCRDRef

    steps_ready: Outcome
    steps: list[FunctionRef]

    completion: celpy.Runner | None
