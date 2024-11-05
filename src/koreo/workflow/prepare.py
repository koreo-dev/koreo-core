from typing import Generator, Coroutine
import logging
import re

import celpy

from koreo.cache import get_resource_from_cache, reprepare_and_update_cache
from koreo.cel.encoder import encode_cel
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.function.registry import index_workflow_functions
from koreo.function.structure import Function
from koreo.result import (
    Ok,
    Outcome,
    PermFail,
    Retry,
    UnwrappedOutcome,
    is_error,
    is_unwrapped_ok,
    unwrapped_combine,
)

from controller.custom_workflow import start_controller

from . import structure
from .registry import (
    index_workflow_custom_crd,
    index_workflow_workflows,
    get_workflow_workflows,
)


async def prepare_workflow(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[tuple[structure.Workflow, Generator[Coroutine, None, None]]]:
    logging.info(f"Prepare workflow {cache_key}")

    if not spec:
        spec = {}

    cel_env = celpy.Environment(annotations=koreo_function_annotations)

    spec_steps = spec.get("steps", [])
    steps, steps_ready = _load_steps(cel_env, spec_steps)

    # Update cross-reference registries
    function_keys = []
    workflow_keys = []
    for step in spec_steps:
        function_ref = step.get("functionRef")
        if function_ref:
            function_keys.append(function_ref.get("name"))

        workflow_ref = step.get("workflowRef")
        if workflow_ref:
            workflow_keys.append(workflow_ref.get("name"))

    index_workflow_functions(workflow=cache_key, functions=function_keys)
    index_workflow_workflows(workflow=cache_key, workflows=workflow_keys)

    # Update CRD registry and ensure controller is running for the CRD.
    crd_ref = _build_crd_ref(spec.get("crdRef", {}))
    index_workflow_custom_crd(
        workflow=cache_key,
        custom_crd=f"{crd_ref.api_group}:{crd_ref.kind}:{crd_ref.version}",
    )

    start_controller(
        group=crd_ref.api_group, kind=crd_ref.kind, version=crd_ref.version
    )

    # Re-prepare any Workflows using this Workflow
    updaters = (
        reprepare_and_update_cache(
            resource_class=structure.Workflow,
            preparer=prepare_workflow,
            cache_key=workflow_key,
        )
        for workflow_key in get_workflow_workflows(workflow=cache_key)
    )

    return (
        structure.Workflow(
            crd_ref=crd_ref,
            steps_ready=steps_ready,
            steps=steps,
            status=_build_status(cel_env=cel_env, status_spec=spec.get("status")),
        ),
        updaters,
    )


def _build_crd_ref(crd_ref_spec: dict) -> structure.ConfigCRDRef:
    return structure.ConfigCRDRef(
        api_group=crd_ref_spec.get("apiGroup"),
        version=crd_ref_spec.get("version"),
        kind=crd_ref_spec.get("kind"),
    )


INPUT_NAME_PATTERN = re.compile("steps.([^.]+).?")


def _load_steps(
    cel_env: celpy.Environment, steps_spec: list[dict]
) -> tuple[list[structure.Step], Outcome]:

    known_steps: set[str] = set()

    step_outcomes: list[UnwrappedOutcome] = []
    for step_spec in steps_spec:
        step_label = step_spec.get("label")
        step_outcomes.append(_load_step(cel_env, step_spec, known_steps))
        known_steps.add(step_label)

    overall_outcome = unwrapped_combine(step_outcomes)

    if is_unwrapped_ok(overall_outcome):
        return step_outcomes, Ok(None)

    return step_outcomes, overall_outcome


def _load_step(cel_env: celpy.Environment, step_spec: dict, known_steps: set[str]):
    step_label = step_spec.get("label")
    if not step_label:
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(message=f"Missing step-label can not prepare Workflow."),
            condition=None,
        )

    logic_cache_key = None
    logic = None

    function_ref = step_spec.get("functionRef")
    if function_ref:
        logic_cache_key, logic = _load_function(step_label, function_ref)

    workflow_ref = step_spec.get("workflowRef")
    if workflow_ref:
        logic_cache_key, logic = _load_workflow(step_label, workflow_ref)

    if is_error(logic):
        return structure.ErrorStep(label=step_label, outcome=logic, condition=None)

    if not (logic_cache_key and logic):
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(
                message=f"Unable to load step ({step_label}), can not prepare Workflow."
            ),
            condition=None,
        )

    dynamic_input_keys = set()
    provided_input_keys = set()

    mapped_input_spec = step_spec.get("mappedInput")
    if not mapped_input_spec:
        mapped_input = None
    else:
        source_iterator = mapped_input_spec.get("source")
        encoded_source_iterator = encode_cel(source_iterator)

        source_iterator_expression = cel_env.compile(encoded_source_iterator)
        used_vars = extract_argument_structure(source_iterator_expression)

        dynamic_input_keys.update(
            INPUT_NAME_PATTERN.match(key).group(1)
            for key in used_vars
            if key.startswith("steps.")
        )
        provided_input_keys.add(mapped_input_spec.get("inputKey"))

        mapped_input = structure.MappedInput(
            source_iterator=cel_env.program(
                source_iterator_expression, functions=koreo_cel_functions
            ),
            input_key=mapped_input_spec.get("inputKey"),
        )

    input_mapper_spec = step_spec.get("inputs")
    if not input_mapper_spec:
        input_mapper = None
    else:
        encoded_input_extractor = encode_cel(input_mapper_spec)

        input_mapper_expression = cel_env.compile(encoded_input_extractor)
        used_vars = extract_argument_structure(input_mapper_expression)

        dynamic_input_keys.update(
            INPUT_NAME_PATTERN.match(key).group(1)
            for key in used_vars
            if key.startswith("steps.")
        )
        provided_input_keys.update(input_mapper_spec.keys())

        input_mapper = cel_env.program(
            input_mapper_expression, functions=koreo_cel_functions
        )

    out_of_order_steps = dynamic_input_keys.difference(known_steps)
    if out_of_order_steps:
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(
                message=f"Function ({logic_cache_key}), must come after {', '.join(out_of_order_steps)}.",
                location=f"{step_label}:{logic_cache_key}",
            ),
            condition=None,
        )

    condition_spec = step_spec.get("condition")
    if not condition_spec:
        condition = None
    else:
        condition = structure.StepConditionSpec(
            type_=condition_spec.get("type"),
            name=condition_spec.get("name"),
        )

    return structure.Step(
        label=step_label,
        logic=logic,
        mapped_input=mapped_input,
        inputs=input_mapper,
        dynamic_input_keys=list(dynamic_input_keys),
        provided_input_keys=provided_input_keys,
        condition=condition,
    )


def _load_function(step_label: str, function_ref: dict):
    function_cache_key = function_ref.get("name")
    if not function_cache_key:
        return function_cache_key, PermFail(
            message=f"Missing Function name, can not prepare Workflow.",
            location=f"{step_label}",
        )

    function = get_resource_from_cache(
        resource_class=Function,
        cache_key=function_cache_key,
    )
    if not function:
        return function_cache_key, Retry(
            message=f"Missing Function ({function_cache_key}), can not prepare Workflow.",
            delay=15,
            location=f"{step_label}:{function_cache_key}",
        )

    if is_error(function):
        return function_cache_key, Retry(
            message=f"Function ({function_cache_key}) is not healthy ({function.message}). Workflow prepare will retry.",
            delay=180,
            location=f"{step_label}:{function_cache_key}",
        )
    return function_cache_key, function


def _load_workflow(step_label: str, workflow_ref: dict):
    workflow_cache_key = workflow_ref.get("name")
    if not workflow_cache_key:
        return workflow_cache_key, PermFail(
            message=f"Missing Workflow name, can not prepare Workflow.",
            location=f"{step_label}",
        )

    workflow = get_resource_from_cache(
        resource_class=structure.Workflow,
        cache_key=workflow_cache_key,
    )
    if not workflow:
        return workflow_cache_key, Retry(
            message=f"Missing Function ({workflow_cache_key}), can not prepare Workflow.",
            delay=15,
            location=f"{step_label}:{workflow_cache_key}",
        )

    if is_error(workflow.steps_ready):
        return workflow_cache_key, Retry(
            message=f"Workflow ({workflow_cache_key}) is not healthy ({workflow.steps_ready.message}). Workflow prepare will retry.",
            delay=180,
            location=f"{step_label}:{workflow_cache_key}",
        )
    return workflow_cache_key, workflow


def _build_status(
    cel_env: celpy.Environment, status_spec: dict | None
) -> structure.Status:
    if not status_spec:
        return structure.Status(conditions=[], state=None)

    state_spec = status_spec.get("state")
    if not state_spec:
        state = None
    else:
        state = cel_env.program(cel_env.compile(encode_cel(state_spec)))

    conditions_spec = status_spec.get("conditions")
    if not conditions_spec:
        conditions = []
    else:
        conditions = [
            structure.ConditionSpec(
                type_=condition_spec.get("type"),
                name=condition_spec.get("name"),
                step=condition_spec.get("step"),
            )
            for condition_spec in conditions_spec
        ]

    return structure.Status(conditions=conditions, state=state)
