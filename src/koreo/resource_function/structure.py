from typing import NamedTuple, Sequence

from kr8s._objects import APIObject

import celpy

from koreo.cel.prepare import Overlay
from koreo.result import UnwrappedOutcome
from koreo.value_function.structure import ValueFunction


class ResourceTemplateRef(NamedTuple):
    name: celpy.Runner | None


class InlineResourceTemplate(NamedTuple):
    template: celpy.Runner | None = None


class ValueFunctionOverlay(NamedTuple):
    overlay: ValueFunction
    skip_if: celpy.Runner | None = None
    inputs: celpy.Runner | None = None


class InlineOverlay(NamedTuple):
    overlay: Overlay
    skip_if: celpy.Runner | None = None


class Create(NamedTuple):
    enabled: bool = True
    delay: int = 30
    overlay: Overlay | None = None


class UpdatePatch(NamedTuple):
    delay: int = 30


class UpdateRecreate(NamedTuple):
    delay: int = 30


class UpdateNever(NamedTuple):
    pass


Update = UpdatePatch | UpdateRecreate | UpdateNever


class DeleteAbandon(NamedTuple):
    pass


class DeleteDestroy(NamedTuple):
    force: bool = False


class CRUDConfig(NamedTuple):
    resource_api: type[APIObject]
    resource_id: celpy.Runner
    own_resource: bool
    readonly: bool

    resource_template: InlineResourceTemplate | ResourceTemplateRef
    overlays: UnwrappedOutcome[Sequence[InlineOverlay | ValueFunctionOverlay]]

    create: Create
    update: Update


class ResourceFunction(NamedTuple):
    name: str
    preconditions: celpy.Runner | None
    local_values: celpy.Runner | None

    crud_config: CRUDConfig

    postconditions: celpy.Runner | None
    return_value: celpy.Runner | None

    dynamic_input_keys: set[str]
