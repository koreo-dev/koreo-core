from typing import NamedTuple

import celpy


class ValueFunction(NamedTuple):
    validators: celpy.Runner | None
    local_values: celpy.Runner | None
    return_value: celpy.Runner | None

    dynamic_input_keys: set[str]
