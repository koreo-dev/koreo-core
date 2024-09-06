from typing import NamedTuple, Literal

import celpy


class Materializers(NamedTuple):
    base: celpy.Runner | None
    on_create: celpy.Runner | None


class Outcome(NamedTuple):
    tests: celpy.Runner | None
    ok_value: celpy.Runner | None


class ManagedCRD(NamedTuple):
    api_version: str
    kind: str
    plural: str
    namespaced: bool


class ManagerBehavior(NamedTuple):
    virtual: bool
    # TODO: add-owner flag
    load: Literal["name", "label-query"]
    update: Literal["patch", "recreate"]
    delete: Literal["destroy", "abandon"]


class ManagedResource(NamedTuple):
    crd: ManagedCRD | None
    behaviors: ManagerBehavior


class Function(NamedTuple):
    managed_resource: ManagedResource

    input_validators: celpy.Runner | None

    materializers: Materializers
    outcome: Outcome
    template: dict | None
