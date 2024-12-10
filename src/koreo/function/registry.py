from collections import defaultdict
from typing import Sequence


__workflow_function_index = defaultdict(set[str])
__function_wokflow_index = defaultdict(set[str])


def index_workflow_functions(workflow: str, functions: set[str]):
    old_functions = __workflow_function_index[workflow]

    for removed_function in old_functions - functions:
        __function_wokflow_index[removed_function].remove(workflow)

    for added_function in functions - old_functions:
        __function_wokflow_index[added_function].add(workflow)

    __workflow_function_index[workflow] = functions


def get_function_workflows(function: str) -> Sequence[str]:
    return list(__function_wokflow_index[function])


def _reset_registry():
    global __workflow_function_index, __function_wokflow_index
    __workflow_function_index = defaultdict(set[str])
    __function_wokflow_index = defaultdict(set[str])
