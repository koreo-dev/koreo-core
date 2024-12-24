from typing import NamedTuple

import celpy
from celpy import celtypes


class ValueFunction(NamedTuple):
    validators: celpy.Runner | None
    constants: celtypes.Value
    return_value: celpy.Runner | None

    dynamic_input_keys: set[str]
