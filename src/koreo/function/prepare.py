from typing import Coroutine, Generator
import logging


from koreo.cache import reprepare_and_update_cache
from koreo.cel.encoder import encode_cel, encode_cel_template
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.result import PermFail, UnwrappedOutcome
from koreo.workflow.prepare import prepare_workflow
from koreo.workflow.structure import Workflow

from . import structure
from .registry import get_function_workflows

import celpy

# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("Environment").setLevel(logging.WARNING)
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)


async def prepare_function(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.Function, Generator[Coroutine, None, None]]]:
    # NOTE: We can try `celpy.Environment(runner_class=celpy.CompiledRunner)`
    # We need to do a safety check to ensure there are no escapes / injections.
    logging.info(f"Prepare function {cache_key}")

    if not spec:
        return PermFail(
            message=f"Missing `spec` for Function '{cache_key}'.",
            location=f"prepare:Function:{cache_key}",
        )

    env = celpy.Environment(annotations=koreo_function_annotations)

    resource_config = None

    static_resource_spec = spec.get("staticResource")
    if static_resource_spec:
        resource_config = structure.StaticResource(
            behavior=_load_behavior(static_resource_spec.get("behavior")),
            managed_resource=_build_managed_resource(
                static_resource_spec.get("managedResource")
            ),
        )

    dynamic_resource_spec = spec.get("dynamicResource")
    if dynamic_resource_spec:
        resource_config = structure.DynamicResource(
            key=env.program(
                env.compile(encode_cel(dynamic_resource_spec.get("key"))),
                functions=koreo_cel_functions,
            )
        )

    if static_resource_spec and dynamic_resource_spec:
        return PermFail(
            message=f"Can not specify static and dynamic resource config for {cache_key}",
            location=f"prepare:Function:{cache_key}",
        )

    if not resource_config:
        resource_config = structure.StaticResource(
            managed_resource=None, behavior=_load_behavior(None)
        )

    used_vars = set[str]()

    input_validators, validated_vars = _predicate_extractor(
        cel_env=env,
        predicate_spec=spec.get("inputValidators"),
    )
    used_vars.update(validated_vars)

    materializers, materializer_vars = _prepare_materializers(
        cel_env=env, materializers=spec.get("materializers")
    )
    used_vars.update(materializer_vars)

    outcome, outcome_vars = _prepare_outcome(cel_env=env, outcome=spec.get("outcome"))
    used_vars.update(outcome_vars)

    # Update Workflows using this Function.
    updaters = (
        reprepare_and_update_cache(
            resource_class=Workflow,
            preparer=prepare_workflow,
            cache_key=workflow_key,
        )
        for workflow_key in get_function_workflows(function=cache_key)
    )

    return (
        structure.Function(
            resource_config=resource_config,
            input_validators=input_validators,
            materializers=materializers,
            outcome=outcome,
            dynamic_input_keys=used_vars,
        ),
        updaters,
    )


def _build_managed_resource(spec: dict | None) -> structure.ManagedResource | None:
    if not spec:
        return None

    kind = spec.get("kind")
    plural = spec.get("plural", f"{kind.lower()}s")

    return structure.ManagedResource(
        api_version=spec.get("apiVersion"),
        kind=kind,
        plural=plural,
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
) -> tuple[structure.Materializers, set[str]]:
    materializer_vars = set[str]()
    if not materializers:
        return structure.Materializers(base=None, on_create=None), materializer_vars

    base_materializer_spec = materializers.get("base")
    base_materializer, base_vars = _template_extractor(
        cel_env=cel_env, template_spec=base_materializer_spec
    )
    materializer_vars.update(base_vars)

    on_create_materializer_spec = materializers.get("onCreate")
    on_create_materializer, on_create_vars = _template_extractor(
        cel_env=cel_env, template_spec=on_create_materializer_spec
    )
    materializer_vars.update(on_create_vars)

    return (
        structure.Materializers(
            base=base_materializer, on_create=on_create_materializer
        ),
        materializer_vars,
    )


def _prepare_outcome(
    cel_env: celpy.Environment, outcome: dict | None
) -> tuple[structure.Outcome, set[str]]:
    outcome_vars = set[str]()
    if not outcome:
        return structure.Outcome(tests=None, ok_value=None), outcome_vars

    tests = None
    test_spec = outcome.get("tests")
    if test_spec:
        tests, test_vars = _predicate_extractor(
            cel_env=cel_env,
            predicate_spec=test_spec,
        )
        outcome_vars.update(test_vars)

    ok_value = None
    ok_value_spec = outcome.get("okValue")
    if ok_value_spec:
        compiled = cel_env.compile(encode_cel(ok_value_spec))

        # TODO: We should inspect this more to map the output structure vs
        # needed values.
        outcome_vars.update(extract_argument_structure(compiled=compiled))

        ok_value = cel_env.program(compiled, functions=koreo_cel_functions)
        ok_value.logger.setLevel(logging.WARNING)

    return structure.Outcome(tests=tests, ok_value=ok_value), outcome_vars


def _template_extractor(
    cel_env: celpy.Environment,
    template_spec: dict | None,
) -> tuple[celpy.Runner | None, set[str]]:
    template_vars = set[str]()
    if not template_spec:
        return None, template_vars

    materializer = encode_cel_template(template_spec=template_spec)

    compiled = cel_env.compile(materializer)

    template_vars.update(extract_argument_structure(compiled=compiled))

    program = cel_env.program(compiled, functions=koreo_cel_functions)
    program.logger.setLevel(logging.WARNING)
    return program, template_vars


def _predicate_extractor(
    cel_env: celpy.Environment,
    predicate_spec: list[dict] | None,
) -> tuple[celpy.Runner | None, set[str]]:
    predicate_vars = set[str]()
    if not predicate_spec:
        return None, predicate_vars

    predicates = encode_cel(predicate_spec)

    tests = f"{predicates}.filter(predicate, predicate.test)"
    compiled = cel_env.compile(tests)

    predicate_vars.update(extract_argument_structure(compiled=compiled))

    program = cel_env.program(compiled, functions=koreo_cel_functions)
    program.logger.setLevel(logging.WARNING)
    return program, predicate_vars
