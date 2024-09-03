from typing import NamedTuple

import celpy

from koreo.function.structure import Function


class ConfigCRDRef(NamedTuple):
    api_group: str
    version: str
    kind: str


class FunctionRef(NamedTuple):
    label: str
    function: Function

    arg_map: celpy.Runner | None


class Workflow(NamedTuple):
    crd_ref: ConfigCRDRef

    steps: list[FunctionRef]

    completion: celpy.Runner | None
