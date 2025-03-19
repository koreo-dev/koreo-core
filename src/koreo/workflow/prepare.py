from typing import Sequence
import logging
import re

import celpy

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
    is_error,
    is_unwrapped_ok,
    unwrapped_combine,
)
from koreo.value_function.structure import ValueFunction

from . import structure

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

    steps_spec = spec.get("steps", [])
    step_resources, steps, steps_ready, parent_properties = _load_steps(
        cel_env, steps_spec
    )
    if step_resources:
        watched_resources.update(step_resources)

    if not steps_spec:
        steps_ready = unwrapped_combine(
            (
                steps_ready,
                PermFail(
                    message=(
                        "No steps specified Workflow at least one step in "
                        "'spec.steps' is required."
                    )
                ),
            )
        )

    crd_ref = _build_crd_ref(spec.get("crdRef", {}))

    return (
        structure.Workflow(
            name=cache_key,
            crd_ref=crd_ref,
            steps_ready=steps_ready,
            steps=steps,
            dynamic_input_keys=parent_properties,
        ),
        tuple(watched_resources),
    )


def _location(cache_key: str, extra: str | None = None) -> str:
    base = f"prepare:Workflow:{cache_key}"
    if not extra:
        return base

    return f"{base}:{extra}"


def _build_crd_ref(crd_ref_spec: dict) -> structure.ConfigCRDRef | None:
    api_group = crd_ref_spec.get("apiGroup")
    version = crd_ref_spec.get("version")
    kind = crd_ref_spec.get("kind")

    if not (api_group and version and kind):
        return None

    return structure.ConfigCRDRef(api_group=api_group, version=version, kind=kind)


PARENT_NAME_PATTERN = re.compile(r"parent.(?P<name>.*)")
STEPS_NAME_PATTERN = re.compile(r"steps.(?P<name>[^.[]+)?\[?.*")


LogicRegistryResource = registry.Resource[
    ValueFunction | ResourceFunction | structure.Workflow
]
Logic = ValueFunction | ResourceFunction | structure.Workflow


def _load_steps(
    cel_env: celpy.Environment,
    steps_spec: list[dict],
) -> tuple[
    set[LogicRegistryResource],
    Sequence[structure.Step | structure.ErrorStep],
    Outcome,
    set[str],
]:
    if not steps_spec:
        return set(), [], Ok(None), set()

    known_steps: set[str] = set()
    parent_properties: set[str] = set()

    resources: set[LogicRegistryResource] = set()
    step_outcomes: list[structure.Step | structure.ErrorStep] = []
    for step_spec in steps_spec:
        step_label: str = step_spec.get("label", "<missing label>")
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
        step_resources, step_logic, needed_parent_properties = _load_step(
            cel_env, step_spec, known_steps
        )
        known_steps.add(step_label)
        parent_properties.update(needed_parent_properties)
        step_outcomes.append(step_logic)
        if step_resources:
            resources.update(step_resources)

    error_outcomes = [
        step.outcome for step in step_outcomes if isinstance(step, structure.ErrorStep)
    ]

    if not error_outcomes:
        return resources, step_outcomes, Ok(None), parent_properties

    return (
        resources,
        step_outcomes,
        unwrapped_combine(error_outcomes),
        parent_properties,
    )


def _load_step(
    cel_env: celpy.Environment, step_spec: dict, known_steps: set[str]
) -> tuple[
    set[LogicRegistryResource] | None, structure.Step | structure.ErrorStep, set[str]
]:
    step_label = step_spec.get("label", "<missing label>")

    step_location = f"spec.steps['{step_label}']"

    needed_steps = set()
    needed_parent_properties = set()

    resources = None
    logic = None

    logic_ref = step_spec.get("ref")
    logic_ref_switch = step_spec.get("refSwitch")
    if logic_ref and logic_ref_switch:
        # This should never happen due to schema validation
        return (
            None,
            structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"spec.steps['{step_label}'] may not specify both `ref` and `refSwitch`, can not prepare Workflow.",
                    location=step_location,
                ),
                condition=None,
            ),
            needed_parent_properties,
        )

    if logic_ref:
        resources, logic = _load_logic(logic_ref=logic_ref, location=step_location)
    elif logic_ref_switch:
        resources, logic = _load_logic_switch(
            cel_env=cel_env, logic_switch=logic_ref_switch, location=step_location
        )
        if is_unwrapped_ok(logic):
            arg_structure = extract_argument_structure(logic.switch_on.ast)

            needed_parent_properties.update(
                match.group("name")
                for match in (PARENT_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

            needed_steps.update(
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

    if step_label == "<missing label>":
        # Note, this should be impossible due to schema validation.
        return (
            resources,
            structure.ErrorStep(
                label="missing",
                outcome=PermFail(
                    message="Missing step-label, can not prepare Workflow.",
                    location=step_location,
                ),
                condition=None,
            ),
            needed_parent_properties,
        )

    if not is_unwrapped_ok(logic):
        return (
            resources,
            structure.ErrorStep(label=step_label, outcome=logic, condition=None),
            needed_parent_properties,
        )

    if not logic:
        return (
            resources,
            structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"Unable to load Logic for step ({step_label}), can not prepare Workflow.",
                    location=step_location,
                ),
                condition=None,
            ),
            needed_parent_properties,
        )

    skip_if_spec = step_spec.get("skipIf")
    match prepare_expression(
        cel_env=cel_env, spec=skip_if_spec, location=f"{step_location}.skipIf"
    ):
        case PermFail() as failure:
            return (
                resources,
                structure.ErrorStep(
                    label=step_label,
                    outcome=failure,
                    condition=None,
                ),
                needed_parent_properties,
            )
        case None:
            skip_if = None
        case celpy.Runner() as skip_if:
            arg_structure = extract_argument_structure(skip_if.ast)

            needed_parent_properties.update(
                match.group("name")
                for match in (PARENT_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

            needed_steps.update(
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

    for_each_spec = step_spec.get("forEach")
    match _prepare_for_each(cel_env=cel_env, step_label=step_label, spec=for_each_spec):
        case structure.ErrorStep() as error:
            return resources, error, needed_parent_properties
        case None:
            for_each = None
        case (structure.ForEach() as for_each, for_each_input_keys):
            needed_parent_properties.update(
                match.group("name")
                for match in (
                    PARENT_NAME_PATTERN.match(key) for key in for_each_input_keys
                )
                if match
            )

            needed_steps.update(
                match.group("name")
                for match in (
                    STEPS_NAME_PATTERN.match(key) for key in for_each_input_keys
                )
                if match
            )

    input_mapper_spec = step_spec.get("inputs")
    match prepare_map_expression(
        cel_env=cel_env, spec=input_mapper_spec, location=f"{step_location}.inputs"
    ):
        case PermFail() as failure:
            return (
                resources,
                structure.ErrorStep(
                    label=step_label,
                    outcome=failure,
                    condition=None,
                ),
                needed_parent_properties,
            )
        case None:
            input_mapper = None
        case celpy.Runner() as input_mapper:
            arg_structure = extract_argument_structure(input_mapper.ast)

            needed_parent_properties.update(
                match.group("name")
                for match in (PARENT_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

            needed_steps.update(
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in arg_structure)
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
        cel_env=cel_env, spec=state_spec, location=f"{step_location}.state"
    ):
        case None:
            state = None
        case celpy.Runner() as state:
            arg_structure = extract_argument_structure(state.ast)

            needed_parent_properties.update(
                match.group("name")
                for match in (PARENT_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )

            needed_steps.update(
                match.group("name")
                for match in (STEPS_NAME_PATTERN.match(key) for key in arg_structure)
                if match
            )
        case PermFail() as failure:
            return (
                resources,
                structure.ErrorStep(
                    label=step_label,
                    outcome=failure,
                    condition=None,
                ),
                needed_parent_properties,
            )

    out_of_order_steps = needed_steps.difference(known_steps)
    if out_of_order_steps:
        return (
            resources,
            structure.ErrorStep(
                label=step_label,
                outcome=PermFail(
                    message=f"Step '{step_label}', must come after {', '.join(out_of_order_steps)}.",
                    location=step_location,
                ),
                condition=None,
            ),
            needed_parent_properties,
        )

    return (
        resources,
        structure.Step(
            label=step_label,
            logic=logic,
            skip_if=skip_if,
            for_each=for_each,
            inputs=input_mapper,
            dynamic_input_keys=needed_steps,
            condition=condition,
            state=state,
        ),
        needed_parent_properties,
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
            dynamic_input_keys = extract_argument_structure(source_iterator.ast)

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

    for_each = structure.ForEach(
        source_iterator=source_iterator,
        input_key=input_key,
        condition=condition,
    )

    return (for_each, dynamic_input_keys)


def _load_logic(
    logic_ref: dict, location: str
) -> tuple[set[LogicRegistryResource] | None, PermFail | Retry | Logic]:
    logic_kind: str | None = logic_ref.get("kind")
    logic_cache_key: str | None = logic_ref.get("name")

    if not logic_kind:
        return (
            None,
            PermFail(
                message=f"Missing `{location}.kind`, can not prepare Workflow.",
                location=f"{location}.<missing>:{logic_cache_key if logic_cache_key else '<missing>'}",
            ),
        )

    if not logic_cache_key:
        return (
            None,
            PermFail(
                message=f"Missing `{location}.name`, can not prepare Workflow.",
                location=f"{location}.{logic_kind}:<missing>",
            ),
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
        return (
            None,
            PermFail(
                message=f"Invalid `{location}.kind` ({logic_kind}), can not prepare Workflow.",
                location=f"{location}.{logic_kind}:{logic_cache_key}",
            ),
        )

    resources = set(
        [
            registry.Resource(
                resource_type=logic_class,
                name=logic_cache_key,
            )
        ]
    )
    logic = get_resource_from_cache(
        resource_class=logic_class,
        cache_key=logic_cache_key,
    )

    if not logic:
        return (
            resources,
            Retry(
                message=f"'{logic_kind}:{logic_cache_key}' not cached; will retry.",
                delay=15,
                location=f"{location}.{logic_kind}:{logic_cache_key}",
            ),
        )

    if not is_unwrapped_ok(logic):
        return (
            resources,
            Retry(
                message=f"'{logic_kind}:{logic_cache_key}' is not healthy ({logic.message}); will retry.",
                delay=180,
                location=f"{location}.{logic_kind}:{logic_cache_key}",
            ),
        )

    return resources, logic


def _load_logic_switch(
    cel_env: celpy.Environment, logic_switch: dict, location: str
) -> tuple[set[LogicRegistryResource] | None, PermFail | Retry | structure.LogicSwitch]:
    dynamic_input_keys: set[str] = set()

    switch_on_spec = logic_switch.get("switchOn")
    match prepare_expression(cel_env=cel_env, spec=switch_on_spec, location=location):
        case None:
            return None, PermFail(
                message=f"Missing `{location}.switchOn`, can not prepare Workflow.",
                location=f"{location}.switchOn",
            )
        case PermFail() as err:
            return None, err
        case celpy.Runner() as switch_on:
            dynamic_input_keys.update(extract_argument_structure(switch_on.ast))

    cases_spec = logic_switch.get("cases")
    if not cases_spec:
        return None, PermFail(
            message=f"Must specify at least one case in `{location}.cases`, can not prepare Workflow.",
            location=f"{location}.cases",
        )

    default_logic = None
    logic_map = {}
    resources = set[LogicRegistryResource]()
    for idx, case_spec in enumerate(cases_spec):
        case = case_spec.get("case")
        is_default = case_spec.get("default")
        if is_default and default_logic:
            return None, PermFail(
                message=f"Only one case in `{location}.cases` may be default, can not prepare Workflow.",
                location=f"{location}.cases[{idx}].default",
            )

        resources, logic = _load_logic(
            logic_ref=case_spec, location=f"{location}[{idx}]"
        )

        logic_map[case] = logic

        if is_unwrapped_ok(logic):
            if isinstance(logic, structure.Workflow):
                dynamic_input_keys.update(
                    f"inputs.{input}" for input in logic.dynamic_input_keys
                )
            else:
                dynamic_input_keys.update(logic.dynamic_input_keys)

        if is_default:
            default_logic = logic

        if resources:
            resources.update(resources)

    functions_ready = unwrapped_combine(logic_map.values())
    if is_error(functions_ready):
        return resources, functions_ready

    # This is just for the type checker
    if not is_unwrapped_ok(default_logic):
        return resources, default_logic

    return resources, structure.LogicSwitch(
        switch_on=switch_on,
        logic_map=logic_map,
        default_logic=default_logic,
        dynamic_input_keys=dynamic_input_keys,
    )
