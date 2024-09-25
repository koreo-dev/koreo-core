import logging

import asyncio

from koreo.result import Ok, PermFail
from koreo.cache import reprepare_and_update_cache
from koreo.cel.encoder import encode_cel, encode_cel_template
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.workflow.prepare import prepare_workflow
from koreo.workflow.structure import Workflow

from . import structure
from .registry import get_function_workflows

import celpy

# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)

__tasks = set()


async def prepare_function(cache_key: str, spec: dict) -> structure.Function:
    # NOTE: We can try `celpy.Environment(runner_class=celpy.CompiledRunner)`
    # We need to do a safety check to ensure there are no escapes / injections.
    logging.info(f"Prepare function {cache_key}")

    if not spec:
        spec = {}

    function_ready = Ok(None)

    env = celpy.Environment(annotations=koreo_function_annotations)

    resource_config = None

    static_resource_spec = spec.get("staticResource")
    if static_resource_spec:
        resource_config = _build_static_resource(static_resource_spec)

    dynamic_resource_spec = spec.get("dynamicResource")
    if dynamic_resource_spec:
        resource_config = structure.DynamicResource(
            key=env.program(
                env.compile(encode_cel(dynamic_resource_spec.get("key"))),
                functions=koreo_cel_functions,
            )
        )

    if static_resource_spec and dynamic_resource_spec:
        function_ready = PermFail(
            f"Can not specify static and dynamic resource config for {cache_key}"
        )

    if not resource_config:
        resource_config = structure.StaticResource(
            managed_resource=None, behavior=_load_behavior(None)
        )

    input_validators = _predicate_extractor(
        cel_env=env,
        predicate_spec=spec.get("inputValidators"),
    )

    materializers = _prepare_materializers(
        cel_env=env, materializers=spec.get("materializers")
    )

    outcome = _prepare_outcome(cel_env=env, outcome=spec.get("outcome"))

    loop = asyncio.get_event_loop()
    for workflow_key in get_function_workflows(function=cache_key):
        workflow_task = loop.create_task(
            reprepare_and_update_cache(
                resource_class=Workflow,
                preparer=prepare_workflow,
                cache_key=workflow_key,
            )
        )
        __tasks.add(workflow_task)
        workflow_task.add_done_callback(__tasks.discard)

    return structure.Function(
        resource_config=resource_config,
        input_validators=input_validators,
        materializers=materializers,
        outcome=outcome,
        function_ready=function_ready,
    )


def _build_static_resource(spec: dict) -> structure.StaticResource:
    return structure.StaticResource(
        behavior=_load_behavior(spec.get("behavior")),
        managed_resource=_build_managed_resource(spec.get("managedResource")),
    )


def _build_managed_resource(spec: dict | None) -> structure.ManagedResource | None:
    if not spec:
        return None

    kind = spec.get("kind")
    plural = spec.get("plural")

    return structure.ManagedResource(
        api_version=spec.get("apiVersion"),
        kind=kind,
        plural=plural if plural else f"{kind.lower()}s",
        namespaced=spec.get("namespaced", True),
    )


def _load_behavior(spec: dict | None) -> structure.Behavior:
    if not spec:
        spec = {}

    return structure.Behavior(
        load=spec.get("load", "name"),
        create=spec.get("create", True),
        update=spec.get("update", "patch"),
        delete=spec.get("delete", "destroy"),
    )


def _prepare_materializers(
    cel_env: celpy.Environment, materializers: dict | None
) -> structure.Materializers:
    if not materializers:
        return structure.Materializers(base=None, on_create=None)

    base_materializer_spec = materializers.get("base")
    base_materializer = _template_extractor(
        cel_env=cel_env, template_spec=base_materializer_spec
    )

    on_create_materializer_spec = materializers.get("onCreate")
    on_create_materializer = _template_extractor(
        cel_env=cel_env, template_spec=on_create_materializer_spec
    )

    return structure.Materializers(
        base=base_materializer, on_create=on_create_materializer
    )


def _prepare_outcome(
    cel_env: celpy.Environment, outcome: dict | None
) -> structure.Outcome:
    if not outcome:
        return structure.Outcome(tests=None, ok_value=None)

    tests = None
    test_spec = outcome.get("tests")
    if test_spec:
        tests = _predicate_extractor(
            cel_env=cel_env,
            predicate_spec=test_spec,
        )

    ok_value = None
    ok_value_spec = outcome.get("okValue")
    if ok_value_spec:
        compiled = cel_env.compile(encode_cel(ok_value_spec))
        ok_value = cel_env.program(compiled, functions=koreo_cel_functions)
        ok_value.logger.setLevel(logging.WARNING)

    return structure.Outcome(tests=tests, ok_value=ok_value)


def _template_extractor(
    cel_env: celpy.Environment,
    template_spec: dict | None,
) -> celpy.Runner | None:
    if not template_spec:
        return None

    materializer = encode_cel_template(template_spec=template_spec)

    compiled = cel_env.compile(materializer)
    program = cel_env.program(compiled, functions=koreo_cel_functions)
    program.logger.setLevel(logging.WARNING)
    return program


def _predicate_extractor(
    cel_env: celpy.Environment,
    predicate_spec: list[dict] | None,
) -> celpy.Runner | None:
    if not predicate_spec:
        return None

    predicates = encode_cel(predicate_spec)

    tests = f"{predicates}.filter(predicate, predicate.test)"
    compiled = cel_env.compile(tests)

    program = cel_env.program(compiled, functions=koreo_cel_functions)
    program.logger.setLevel(logging.WARNING)
    return program
