from collections import defaultdict


__workflow_custom_crd_index = defaultdict(str)
__custom_crd_wokflow_index = defaultdict(set[str])

__workflow_workflow_index = defaultdict(set[str])
__workflow_wokflow_index = defaultdict(set[str])


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


def get_custom_crd_workflows(custom_crd: str) -> list[str]:
    return list(__custom_crd_wokflow_index[custom_crd])


def index_workflow_workflows(workflow: str, workflows: set[str]):
    old_workflows = __workflow_workflow_index[workflow]

    for removed_workflow in old_workflows - workflows:
        __workflow_wokflow_index[removed_workflow].remove(workflow)

    for added_workflow in workflows - old_workflows:
        __workflow_wokflow_index[added_workflow].add(workflow)

    __workflow_workflow_index[workflow] = workflows


def get_workflow_workflows(workflow: str) -> list[str]:
    return list(__workflow_wokflow_index[workflow])


def _reset_registry():
    global __workflow_custom_crd_index, __custom_crd_wokflow_index
    __workflow_custom_crd_index = defaultdict(str)
    __custom_crd_wokflow_index = defaultdict(set[str])

    global __workflow_workflow_index, __workflow_wokflow_index
    __workflow_workflow_index = defaultdict(set[str])
    __workflow_wokflow_index = defaultdict(set[str])
