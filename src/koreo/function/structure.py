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
    load: Literal["name", "label-query", "virtual"]
    create: bool
    update: Literal["patch", "recreate", "never"]
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
