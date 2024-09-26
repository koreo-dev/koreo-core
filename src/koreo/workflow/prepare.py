import logging
import re

import celpy

from koreo.cache import get_resource_from_cache
from koreo.cel.encoder import encode_cel
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.function.registry import index_workload_functions
from koreo.function.structure import Function
from koreo.result import Ok, Outcome, PermFail, Retry, combine

from controller.custom_workflow import start_controller

from . import structure
from .registry import index_workload_custom_crd


async def prepare_workflow(cache_key: str, spec: dict | None) -> structure.Workflow:
    logging.info(f"Prepare workflow {cache_key}")

    if not spec:
        spec = {}

    cel_env = celpy.Environment(annotations=koreo_function_annotations)

    spec_steps = spec.get("steps", [])
    steps, steps_ready = _load_functions(cel_env, spec_steps)

    function_keys = []
    for step in spec_steps:
        ref = step.get("functionRef", {})
        function_keys.append(ref.get("name"))

    index_workload_functions(
        workflow=cache_key,
        functions=function_keys,
    )

    crd_ref = _build_crd_ref(spec.get("crdRef", {}))
    index_workload_custom_crd(
        workflow=cache_key,
        custom_crd=f"{crd_ref.api_group}:{crd_ref.kind}:{crd_ref.version}",
    )

    start_controller(
        group=crd_ref.api_group, kind=crd_ref.kind, version=crd_ref.version
    )

    return structure.Workflow(
        crd_ref=crd_ref,
        steps_ready=steps_ready,
        steps=steps,
        status=_build_status(cel_env=cel_env, status_spec=spec.get("status")),
    )


def _build_crd_ref(crd_ref_spec: dict) -> structure.ConfigCRDRef:
    return structure.ConfigCRDRef(
        api_group=crd_ref_spec.get("apiGroup"),
        version=crd_ref_spec.get("version"),
        kind=crd_ref_spec.get("kind"),
    )


INPUT_NAME_PATTERN = re.compile("steps.([^.]+).?")


def _load_functions(
    cel_env: celpy.Environment, step_spec: list[dict]
) -> tuple[list[structure.FunctionRef], Outcome]:
    known_steps: set[str] = set()

    outcomes: list[Outcome] = []

    functions = []
    for step in step_spec:
        function_ref = step.get("functionRef", {})
        function_cache_key = function_ref.get("name")
        function = get_resource_from_cache(
            resource_type=Function,
            cache_key=function_cache_key,
        )
        if not function:
            outcomes.append(
                Retry(
                    message=f"Missing Function ({function_cache_key}), can not prepare Workflow.",
                    delay=15,
                )
            )
            continue

        dynamic_input_keys = set()

        mapped_input_spec = step.get("mappedInput")
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

            mapped_input = structure.MappedInput(
                source_iterator=cel_env.program(
                    source_iterator_expression, functions=koreo_cel_functions
                ),
                input_key=mapped_input_spec.get("inputKey"),
            )

        input_mapper_spec = step.get("inputs")
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

            input_mapper = cel_env.program(
                input_mapper_expression, functions=koreo_cel_functions
            )

        out_of_order_steps = dynamic_input_keys.difference(known_steps)
        if out_of_order_steps:
            outcomes.append(
                PermFail(
                    message=f"Function ({function_cache_key}), must come after {', '.join(out_of_order_steps)}.",
                )
            )
            continue

        step_label = step.get("label")
        known_steps.add(step_label)

        outcomes.append(Ok(None))
        functions.append(
            structure.FunctionRef(
                label=step_label,
                function=function,
                mapped_input=mapped_input,
                inputs=input_mapper,
                dynamic_input_keys=list(dynamic_input_keys),
            )
        )

    return functions, combine(outcomes)


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
                type_=condition_spec.type,
                name=condition_spec.name,
                step=condition_spec.step,
            )
            for condition_spec in conditions_spec
        ]

    return structure.Status(conditions=conditions, state=state)
