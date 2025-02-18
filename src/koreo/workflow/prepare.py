from typing import Sequence
import asyncio
import logging
import re
import time

import celpy

from koreo import ref_helpers
from koreo import registry
from koreo import schema
from koreo.cache import get_resource_from_cache
from koreo.cel.functions import koreo_function_annotations
from koreo.cel.prepare import prepare_expression, prepare_map_expression
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.resource_function.structure import ResourceFunction
from koreo.result import (
    Ok,
    Outcome,
    PermFail,
    Retry,
    UnwrappedOutcome,
    is_unwrapped_ok,
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
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.Workflow, Sequence[registry.Resource]]]:
    logger.debug(f"Prepare workflow {cache_key}")

    if error := schema.validate(
        resource_type=structure.Workflow, spec=spec, validation_required=True
    ):
        return PermFail(
            message=error.message,
            location=_location(cache_key, "spec"),
        )

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
        location_base = _location(cache_key, "spec.configStep")
        match ref_helpers.function_or_workflow_to_resource(
            config_step_spec, location=location_base
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
                step, location=location_base
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
        ),
        tuple(watched_resources),
    )


def _location(cache_key: str, extra: str | None = None) -> str:
    base = f"prepare:Workflow:{cache_key}"
    if not extra:
        return base

    return f"{base}:{extra}"


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

    logic_ref = step_spec.get("ref")
    if logic_ref:
        logic_cache_key, logic = _load_logic(step_label, logic_ref)

    if not is_unwrapped_ok(logic):
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
    match prepare_map_expression(
        cel_env=cel_env, spec=input_mapper_spec, location="spec.configStep.inputs"
    ):
        case None:
            input_mapper = None
            dynamic_input_keys: set[str] = set()
        case celpy.Runner() as input_mapper:
            used_vars = extract_argument_structure(input_mapper.ast)
            dynamic_input_keys: set[str] = {
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
                if match
            }
        case PermFail(message=message):
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=message,
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

    state_spec = step_spec.get("state")
    match prepare_map_expression(
        cel_env=cel_env, spec=state_spec, location="spec.configStep.state"
    ):
        case None:
            state = None
        case celpy.Runner() as state:
            used_vars = extract_argument_structure(state.ast)
            dynamic_input_keys.update(
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
                if match
            )
        case PermFail(message=message):
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=message,
                    location=f"{step_label}:{logic_cache_key}",
                ),
                condition=None,
            )

    if dynamic_input_keys:
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(
                message=f"Config step ('{step_label}'), may not reference "
                f"steps ({dynamic_input_keys}). Can not prepare Workflow."
            ),
            condition=None,
        )

    return structure.ConfigStep(
        label=step_label,
        logic=logic,
        inputs=input_mapper,
        condition=condition,
        state=state,
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
            outcome=PermFail(message="Missing step-label, can not prepare Workflow."),
            condition=None,
        )

    logic_cache_key = None
    logic = None

    logic_ref = step_spec.get("ref")
    if logic_ref:
        logic_cache_key, logic = _load_logic(step_label, logic_ref)

    if not is_unwrapped_ok(logic):
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

    skip_if_spec = step_spec.get("skipIf")
    match prepare_expression(
        cel_env=cel_env, spec=skip_if_spec, location=f"{step_label}.skipIf"
    ):
        case PermFail() as failure:
            return structure.ErrorStep(
                label=step_label,
                outcome=failure,
                condition=None,
            )
        case None:
            skip_if = None
        case celpy.Runner() as skip_if:
            dynamic_input_keys.update(
                match.group("name")
                for match in (
                    STEPS_NAME_PATTERN.match(key)
                    for key in extract_argument_structure(skip_if.ast)
                )
                if match
            )

    for_each_spec = step_spec.get("forEach")
    match _prepare_for_each(cel_env=cel_env, step_label=step_label, spec=for_each_spec):
        case structure.ErrorStep() as error:
            return error
        case None:
            for_each = None
        case (structure.ForEach() as for_each, for_each_input_keys):
            dynamic_input_keys.update(for_each_input_keys)

    input_mapper_spec = step_spec.get("inputs")
    match prepare_map_expression(
        cel_env=cel_env, spec=input_mapper_spec, location=f"{step_label}.inputs"
    ):
        case PermFail() as failure:
            return structure.ErrorStep(
                label=step_label,
                outcome=failure,
                condition=None,
            )
        case None:
            input_mapper = None
        case celpy.Runner() as input_mapper:
            dynamic_input_keys.update(
                match.group("name")
                for match in (
                    STEPS_NAME_PATTERN.match(key)
                    for key in extract_argument_structure(input_mapper.ast)
                )
                if match
            )

    condition_spec = step_spec.get("condition")
    if not condition_spec:
        condition = None
    else:
        condition = structure.StepConditionSpec(
            type_=condition_spec.get("type"),
            name=condition_spec.get("name"),
        )

    state_spec = step_spec.get("state")
    match prepare_map_expression(
        cel_env=cel_env, spec=state_spec, location=f"step:{step_label}.state"
    ):
        case None:
            state = None
        case celpy.Runner() as state:
            dynamic_input_keys.update(
                match.group("name")
                for match in (
                    STEPS_NAME_PATTERN.match(key)
                    for key in extract_argument_structure(state.ast)
                )
                if match
            )
        case PermFail() as failure:
            return structure.ErrorStep(
                label=step_label,
                outcome=failure,
                condition=None,
            )

    out_of_order_steps = dynamic_input_keys.difference(known_steps)
    if out_of_order_steps:
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(
                message=f"Step ({logic_cache_key}), must come after {', '.join(out_of_order_steps)}.",
                location=f"{step_label}:{logic_cache_key}",
            ),
            condition=None,
        )

    return structure.Step(
        label=step_label,
        logic=logic,
        skip_if=skip_if,
        for_each=for_each,
        inputs=input_mapper,
        dynamic_input_keys=tuple(dynamic_input_keys),
        condition=condition,
        state=state,
    )


def _prepare_for_each(
    cel_env: celpy.Environment,
    step_label: str,
    spec: dict | None,
) -> None | structure.ErrorStep | tuple[structure.ForEach, set[str]]:
    if not spec:
        return None

    source_iterator_spec = spec.get("itemIn")
    match prepare_expression(
        cel_env=cel_env,
        spec=source_iterator_spec,
        location=f"{step_label}.forEach.itemIn",
    ):
        case None:
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"Empty `{step_label}.forEach.itemIn`, can not prepare Workflow."
                ),
                condition=None,
            )
        case PermFail(message=message):
            return structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"Error preparing `{step_label}.forEach.itemIn`: {message}."
                ),
                condition=None,
            )
        case celpy.Runner() as source_iterator:
            used_vars = extract_argument_structure(source_iterator.ast)

    input_key = spec.get("inputKey")
    if not input_key:
        return structure.ErrorStep(
            label=step_label,
            outcome=PermFail(
                message=f"Missing `{step_label}.forEach.inputKey`, can not prepare Workflow."
            ),
            condition=None,
        )

    condition_spec = spec.get("condition")
    if not condition_spec:
        condition = None
    else:
        condition = structure.StepConditionSpec(
            type_=condition_spec.get("type"),
            name=condition_spec.get("name"),
        )

    dynamic_input_keys: set[str] = {
        match.group("name")
        for match in (STEPS_NAME_PATTERN.match(key) for key in used_vars)
        if match
    }

    for_each = structure.ForEach(
        source_iterator=source_iterator,
        input_key=input_key,
        condition=condition,
    )

    return (for_each, dynamic_input_keys)


def _load_logic(step_label: str, logic_ref: dict):
    logic_kind = logic_ref.get("kind")
    logic_cache_key = logic_ref.get("name")

    if not logic_kind:
        return logic_cache_key, PermFail(
            message=f"Missing `{step_label}.ref.kind`, can not prepare Workflow.",
            location=f"{step_label}:ref.<missing>:{logic_cache_key if logic_cache_key else '<missing>'}",
        )

    if not logic_cache_key:
        return logic_cache_key, PermFail(
            message=f"Missing `{step_label}.ref.name`, can not prepare Workflow.",
            location=f"{step_label}:ref.{logic_kind}:<missing>",
        )

    logic_kind_map: dict[
        str, type[ValueFunction | ResourceFunction | structure.Workflow]
    ] = {
        "ValueFunction": ValueFunction,
        "ResourceFunction": ResourceFunction,
        "Workflow": structure.Workflow,
    }

    logic_class = logic_kind_map.get(logic_kind)
    if not logic_class:
        return logic_cache_key, PermFail(
            message=f"Invalid `{step_label}.ref.kind` ({logic_kind}), can not prepare Workflow.",
            location=f"{step_label}:ref.{logic_kind}:{logic_cache_key}",
        )

    logic = get_resource_from_cache(
        resource_class=logic_class,
        cache_key=logic_cache_key,
    )

    if not logic:
        return logic_cache_key, Retry(
            message=f"Missing {logic_kind}:{logic_cache_key}. Workflow prepare will retry.",
            delay=15,
            location=f"{step_label}:ref.{logic_kind}:{logic_cache_key}",
        )

    if not is_unwrapped_ok(logic):
        return logic_cache_key, Retry(
            message=f"{logic_kind}:{logic_cache_key} is not healthy ({logic.message}). Workflow prepare will retry.",
            delay=180,
            location=f"{step_label}:{logic_kind}:{logic_cache_key}",
        )

    return logic_cache_key, logic
