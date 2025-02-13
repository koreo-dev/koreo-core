from typing import NamedTuple

import celpy
from koreo.cel.prepare import Overlay


class ValueFunction(NamedTuple):
    preconditions: celpy.Runner | None
    local_values: celpy.Runner | None
    return_value: Overlay | None

    dynamic_input_keys: set[str]
