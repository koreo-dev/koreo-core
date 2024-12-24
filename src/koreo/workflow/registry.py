from collections import defaultdict


__workflow_custom_crd_index = defaultdict(str)
__custom_crd_wokflow_index = defaultdict(set[str])


def index_workflow_custom_crd(workflow: str, custom_crd: str):
    prior_custom_crd = __workflow_custom_crd_index[workflow]

    if prior_custom_crd == custom_crd:
        return

    if workflow in __custom_crd_wokflow_index[prior_custom_crd]:
        __custom_crd_wokflow_index[prior_custom_crd].remove(workflow)

    __custom_crd_wokflow_index[custom_crd].add(workflow)

    __workflow_custom_crd_index[workflow] = custom_crd


def unindex_workflow_custom_crd(workflow: str):
    prior_custom_crd = __workflow_custom_crd_index[workflow]

    if not prior_custom_crd:
        return

    if workflow in __custom_crd_wokflow_index[prior_custom_crd]:
        __custom_crd_wokflow_index[prior_custom_crd].remove(workflow)

    del __workflow_custom_crd_index[workflow]


def get_custom_crd_workflows(custom_crd: str) -> list[str]:
    return list(__custom_crd_wokflow_index[custom_crd])


def _reset_registry():
    global __workflow_custom_crd_index, __custom_crd_wokflow_index
    __workflow_custom_crd_index = defaultdict(str)
    __custom_crd_wokflow_index = defaultdict(set[str])
