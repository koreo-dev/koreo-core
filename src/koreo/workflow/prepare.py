import celpy
import logging

from koreo.cache import get_resource_from_cache
from koreo.function.structure import Function

from koreo.cache import build_cache_key
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
    steps = _load_functions(cel_env, spec_steps)

    function_keys = []
    for step in spec_steps:
        ref = step.get("functionRef", {})
        function_keys.append(
            build_cache_key(name=ref.get("name"), version=ref.get("version"))
        )

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
) -> list[structure.FunctionRef]:
    known_steps: set[str] = set()

    functions = []
    for step in step_spec:
        function_ref = step.get("functionRef", {})
        function = get_resource_from_cache(
            resource_type=Function,
            cache_key=build_cache_key(
                name=function_ref.get("name"), version=function_ref.get("version")
            ),
        )
        if not function:
            raise Exception("Missing function")

        input_mapper_spec = step.get("inputs")
        if not input_mapper_spec:
            input_mapper = None
            dynamic_input_keys = []
        else:
            dynamic_input_keys = input_mapper_spec.keys()
            if set(dynamic_input_keys).difference(known_steps):
                raise Exception("Steps must be ordered!")
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

        functions.append(
            structure.FunctionRef(
                label=step_label,
                function=function,
                inputs=input_mapper,
                dynamic_input_keys=dynamic_input_keys,
                static_inputs=static_inputs,
            )
        )

    return functions


def _build_completion(
    cel_env: celpy.Environment, completion_spec: dict
) -> celpy.Runner | None:
    return None
