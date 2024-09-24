from typing import NamedTuple, Literal

import celpy


class Materializers(NamedTuple):
    base: celpy.Runner | None
    on_create: celpy.Runner | None


class Outcome(NamedTuple):
    tests: celpy.Runner | None
    ok_value: celpy.Runner | None


class ManagedResource(NamedTuple):
    api_version: str
    kind: str
    plural: str
    namespaced: bool


class Behavior(NamedTuple):
    load: Literal["name", "label-query", "virtual"]
    create: bool
    update: Literal["patch", "recreate", "never"]
    delete: Literal["destroy", "abandon"]


class Function(NamedTuple):
    managed_resource: ManagedResource | None
    behavior: Behavior

    input_validators: celpy.Runner | None

    materializers: Materializers
    outcome: Outcome
    template: dict | None
