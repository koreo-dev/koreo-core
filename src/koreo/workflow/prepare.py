from typing import Sequence
import asyncio
import logging
import re
import time

import celpy

from koreo import ref_helpers
from koreo import registry
from koreo.cache import get_resource_from_cache
from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.function.structure import Function
from koreo.result import (
    Ok,
    Outcome,
    PermFail,
    Retry,
    UnwrappedOutcome,
    is_error,
    unwrapped_combine,
)
from koreo.value_function.structure import ValueFunction

from controller.custom_workflow import start_controller

from . import structure
from .registry import (
    index_workflow_custom_crd,
    unindex_workflow_custom_crd,
)


logger = logging.getLogger("koreo.workflow")


async def prepare_workflow(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[tuple[structure.Workflow, Sequence[registry.Resource]]]:
    logger.info(f"Prepare workflow {cache_key}")

    if not spec:
        spec = {}

    cel_env = celpy.Environment(annotations=koreo_function_annotations)

    # Used to  update our cross-reference registries
    watched_resources = set[registry.Resource]()

    # Handle configStep, which is just slightly special.
    config_step_spec = spec.get("configStep", {})
    config_step = _load_config_step(cel_env, config_step_spec)

    if config_step:
        config_step_label = config_step.label
    else:
        config_step_label = None

    if config_step_spec:
        location_base = f"Workflow:{cache_key}:configStep"
        match ref_helpers.function_or_workflow_to_resource(
            config_step_spec, location_base=location_base
        ):
            case None:
                return PermFail(
                    message="`configStep` must contain `functionRef` or `workflowRef`",
                    location=location_base,
                )
            case PermFail() as perm_fail:
                return perm_fail

            case registry.Resource() as resource:
                watched_resources.add(resource)

    # Handle normal Steps
    steps_spec = spec.get("steps", [])
    steps, steps_ready = _load_steps(
        cel_env, steps_spec, config_step_label=config_step_label
    )

    # Ensure configStep is checked as well.
    if isinstance(config_step, structure.ErrorStep):
        all_steps_ready = unwrapped_combine((config_step.outcome, steps_ready))
    else:
        all_steps_ready = steps_ready

    if not (steps_spec or config_step_spec):
        all_steps_ready = unwrapped_combine(
            (
                all_steps_ready,
                PermFail(
                    message=(
                        "No steps specified Workflow, either 'spec.configStep' or "
                        "at least one step in 'spec.steps' is required."
                    )
                ),
            )
        )

    # Perform registry updates.
    if steps_spec:
        for step in steps_spec:
            step_label = step.get("label")
            location_base = f"Workflow:{cache_key}:{step_label}"
            match ref_helpers.function_or_workflow_to_resource(
                step, location_base=location_base
            ):
                case None:
                    return PermFail(
                        message=f"Step `{step_label}` must contain `functionRef` or `workflowRef`",
                        location=location_base,
                    )
                case PermFail() as perm_fail:
                    return perm_fail

                case registry.Resource() as resource:
                    watched_resources.add(resource)

    # Update CRD registry and ensure controller is running for the CRD.
    crd_ref = _build_crd_ref(spec.get("crdRef", {}))
    if not crd_ref:
        unindex_workflow_custom_crd(workflow=cache_key)
    else:
        deletor_name = f"DeleteWorkflow:{cache_key}"
        if deletor_name not in _DEREGISTERERS:
            delete_task = asyncio.create_task(
                _deindex_crd_on_delete(cache_key=cache_key), name=deletor_name
            )
            _DEREGISTERERS[deletor_name] = delete_task
            delete_task.add_done_callback(
                lambda task: _DEREGISTERERS.__delitem__(task.get_name())
            )

        index_workflow_custom_crd(
            workflow=cache_key,
            custom_crd=f"{crd_ref.api_group}:{crd_ref.kind}:{crd_ref.version}",
        )

        start_controller(
            group=crd_ref.api_group, kind=crd_ref.kind, version=crd_ref.version
        )

    return (
        structure.Workflow(
            crd_ref=crd_ref,
            config_step=config_step,
            steps_ready=all_steps_ready,
            steps=steps,
            status=_build_status(cel_env=cel_env, status_spec=spec.get("status")),
        ),
        tuple(watched_resources),
    )


class WorkflowDeleteor: ...


_DEREGISTERERS: dict[str, asyncio.Task] = {}


async def _deindex_crd_on_delete(cache_key: str):
    deletor_resource = registry.Resource(
        resource_type=WorkflowDeleteor, name=cache_key, namespace=None
    )
    queue = registry.register(deletor_resource)

    registry.subscribe(
        subscriber=deletor_resource,
        resource=registry.Resource(
            resource_type=structure.Workflow, name=cache_key, namespace=None
        ),
    )

    last_event = 0
    while True:
        try:
            event = await queue.get()
        except (asyncio.CancelledError, asyncio.QueueShutDown):
            break

        try:
            match event:
                case registry.Kill():
                    break
                case registry.ResourceEvent(
                    event_time=event_time
                ) if event_time >= last_event:
                    cached = get_resource_from_cache(
                        resource_class=structure.Workflow, cache_key=cache_key
                    )

                    if cached:
                        continue

                    logger.debug(f"Deregistering CRD watches for Workflow {cache_key}")

                    unindex_workflow_custom_crd(workflow=cache_key)

                    break

        finally:
            queue.task_done()

    registry.deregister(deletor_resource, deregistered_at=time.monotonic())


def _build_crd_ref(crd_ref_spec: dict) -> structure.ConfigCRDRef | None:
    api_group = crd_ref_spec.get("apiGroup")
    version = crd_ref_spec.get("version")
    kind = crd_ref_spec.get("kind")

    if not (api_group and version and kind):
        return None

    return structure.ConfigCRDRef(api_group=api_group, version=version, kind=kind)


STEPS_NAME_PATTERN = re.compile(r"steps.(?P<name>[^.[]+)?\[?.*")


def _load_config_step(
    cel_env: celpy.Environment, step_spec: dict
) -> None | structure.ConfigStep | structure.ErrorStep:
    if not step_spec:
        return None

    step_label = step_spec.get("label", "config")
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
                message=f"Unable to load '{step_label}', can not prepare Workflow."
            ),
            condition=None,
        )

    input_mapper_spec = step_spec.get("inputs")
    if not input_mapper_spec:
        input_mapper = None
    else:
        encoded_input_extractor = encode_cel(input_mapper_spec)

        try:
            input_mapper_expression = cel_env.compile(encoded_input_extractor)
        except celpy.CELParseError as err:
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"CELParseError {err} parsing inputs ({encoded_input_extractor})",
                    location=f"{step_label}:{logic_cache_key}",
                ),
                condition=None,
            )

        used_vars = extract_argument_structure(input_mapper_expression)

        dynamic_input_keys: set[str] = {
            match.group("name")
            for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
            if match
        }
        if dynamic_input_keys:
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"Config step ('{step_label}'), may not reference "
                    f"steps ({dynamic_input_keys}). Can not prepare Workflow."
                ),
                condition=None,
            )

        input_mapper = cel_env.program(
            input_mapper_expression, functions=koreo_cel_functions
        )

    condition_spec = step_spec.get("condition")
    if not condition_spec:
        condition = None
    else:
        condition = structure.StepConditionSpec(
            type_=condition_spec.get("type"),
            name=condition_spec.get("name"),
        )

    return structure.ConfigStep(
        label=step_label,
        logic=logic,
        inputs=input_mapper,
        condition=condition,
    )


def _load_steps(
    cel_env: celpy.Environment,
    steps_spec: list[dict],
    config_step_label: str | None = None,
) -> tuple[list[structure.Step | structure.ErrorStep], Outcome]:
    if not steps_spec:
        return [], Ok(None)

    known_steps: set[str] = set()
    if config_step_label:
        known_steps.add(config_step_label)

    step_outcomes: list[structure.Step | structure.ErrorStep] = []
    for step_spec in steps_spec:
        step_label = step_spec.get("label")
        if step_label in known_steps:
            step_outcomes.append(
                structure.ErrorStep(
                    label=step_label,
                    outcome=PermFail(
                        message=f"Duplicate step-label ({step_label}) can not prepare Workflow."
                    ),
                    condition=None,
                )
            )
            continue
        step_outcomes.append(_load_step(cel_env, step_spec, known_steps))
        known_steps.add(step_label)

    error_outcomes = [
        step.outcome for step in step_outcomes if isinstance(step, structure.ErrorStep)
    ]

    if not error_outcomes:
        return step_outcomes, Ok(None)

    return step_outcomes, unwrapped_combine(error_outcomes)


def _load_step(cel_env: celpy.Environment, step_spec: dict, known_steps: set[str]):
    step_label = step_spec.get("label")
    if not step_label:
        return structure.ErrorStep(
            label="missing",
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

    mapped_input_spec = step_spec.get("mappedInput")
    if not mapped_input_spec:
        mapped_input = None
        dynamic_input_keys = set()
    else:
        source_iterator = mapped_input_spec.get("source")
        encoded_source_iterator = encode_cel(source_iterator)

        source_iterator_expression = cel_env.compile(encoded_source_iterator)
        used_vars = extract_argument_structure(source_iterator_expression)

        dynamic_input_keys: set[str] = {
            match.group("name")
            for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
            if match
        }

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

        try:
            input_mapper_expression = cel_env.compile(encoded_input_extractor)
        except celpy.CELParseError as err:
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"CELParseError {err} parsing inputs ({encoded_input_extractor})",
                    location=f"{step_label}:{logic_cache_key}",
                ),
                condition=None,
            )

        used_vars = extract_argument_structure(input_mapper_expression)

        dynamic_input_keys.update(
            match.group("name")
            for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
            if match
        )

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
        dynamic_input_keys=tuple(dynamic_input_keys),
        condition=condition,
    )


def _load_function(step_label: str, function_ref: dict):
    function_kind = function_ref.get("kind")
    function_cache_key = function_ref.get("name")

    if not function_kind:
        return function_cache_key, PermFail(
            message=f"Missing functionRef Kind, can not prepare Workflow.",
            location=f"{step_label}:{function_cache_key if function_cache_key else 'missing'}",
        )

    if not function_cache_key:
        return function_cache_key, PermFail(
            message=f"Missing Function name, can not prepare Workflow.",
            location=f"{step_label}:missing",
        )

    if function_kind == "ValueFunction":
        function = get_resource_from_cache(
            resource_class=ValueFunction,
            cache_key=function_cache_key,
        )
    elif function_kind == "Function":
        function = get_resource_from_cache(
            resource_class=Function,
            cache_key=function_cache_key,
        )
    else:
        return function_cache_key, PermFail(
            message=f"Invalid Function kind ({function_kind}), can not prepare Workflow.",
            location=f"{step_label}:{function_cache_key}",
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
            message=f"Missing Workflow ({workflow_cache_key}), can not prepare Workflow.",
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
