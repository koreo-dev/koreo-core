import celpy
import logging

from koreo.cache import get_resource_from_cache
from koreo.function.structure import Function
from koreo.result import Ok, Outcome, PermFail, Retry, combine

from koreo.function.registry import index_workload_functions

from controller.custom_workflow import start_controller

from .registry import index_workload_custom_crd
from . import structure


async def prepare_workflow(cache_key: str, spec: dict | None) -> structure.Workflow:
    logging.info(f"Prepare workflow {cache_key}")

    if not spec:
        spec = {}

    cel_env = celpy.Environment()

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
        completion=_build_completion(cel_env, spec.get("completion", {})),
    )


def _build_crd_ref(crd_ref_spec: dict) -> structure.ConfigCRDRef:
    return structure.ConfigCRDRef(
        api_group=crd_ref_spec.get("apiGroup"),
        version=crd_ref_spec.get("version"),
        kind=crd_ref_spec.get("kind"),
    )


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

        input_mapper_spec = step.get("inputs")
        if not input_mapper_spec:
            input_mapper = None
            dynamic_input_keys = []
        else:
            dynamic_input_keys = input_mapper_spec.keys()
            out_of_order_steps = set(dynamic_input_keys).difference(known_steps)
            if out_of_order_steps:
                outcomes.append(
                    PermFail(
                        message=f"Function ({function_cache_key}), must come after {', '.join(out_of_order_steps)}.",
                    )
                )
                continue

            input_mapper = None
            # print(f"*******************DYNAMIC_INPUT_KEYS  {dynamic_input_keys}")
            # input_mapper_expression = cel_env.compile(input_mapper_spec)
            # print(
            #     f"*******************INPUT_MAPPER_EXPRESSION {input_mapper_expression}"
            # )
            # input_mapper = cel_env.program(input_mapper_expression)

        step_label = step.get("label")
        known_steps.add(step_label)

        static_inputs = step.get("staticInputs") or {}

        outcomes.append(Ok(None))
        functions.append(
            structure.FunctionRef(
                label=step_label,
                function=function,
                inputs=input_mapper,
                dynamic_input_keys=dynamic_input_keys,
                static_inputs=static_inputs,
            )
        )

    return functions, combine(outcomes)


def _build_completion(
    cel_env: celpy.Environment, completion_spec: dict
) -> celpy.Runner | None:
    return None
