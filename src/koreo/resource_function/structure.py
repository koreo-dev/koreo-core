from typing import NamedTuple

from kr8s._objects import APIObject

import celpy


class ResourceTemplateRef(NamedTuple):
    name: celpy.Runner | None


class Base(NamedTuple):
    from_template: celpy.Runner | None = None
    overlay: celpy.Runner | None = None


class Create(NamedTuple):
    enabled: bool = True
    delay: int = 30
    overlay: celpy.Runner | None = None


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


class Outcome(NamedTuple):
    validators: celpy.Runner | None
    return_value: celpy.Runner | None


class CRUDConfig(NamedTuple):
    resource_api: type[APIObject]
    resource_id: celpy.Runner
    own_resource: bool

    base: Base
    create: Create
    update: Update


class ResourceFunction(NamedTuple):
    input_validators: celpy.Runner | None
    local_values: celpy.Runner | None

    crud_config: CRUDConfig

    outcome: Outcome

    dynamic_input_keys: set[str]
