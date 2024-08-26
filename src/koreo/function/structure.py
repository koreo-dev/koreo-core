from typing import NamedTuple

import celpy


class Materializers(NamedTuple):
    base: celpy.Runner | None
    on_create: celpy.Runner | None


class Outcome(NamedTuple):
    tests: celpy.Runner | None
    ok_value: celpy.Runner | None


class Function(NamedTuple):
    input_validators: celpy.Runner | None

    materializers: Materializers
    outcome: Outcome
    template: dict | None
