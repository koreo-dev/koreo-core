from typing import Any, Callable
import logging

import asyncio

from koreo.cache import get_spec_from_cache
from koreo.workflow.prepare import prepare_workflow
from koreo.workflow.structure import Workflow


from .registry import get_function_workflows
from . import structure

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

    managed_resource_spec = spec.get("managedResource")
    managed_resource = _build_resource_settings(spec=managed_resource_spec)

    env = celpy.Environment()

    input_validators = _predicate_extractor(
        cel_env=env,
        predicate_spec=spec.get("inputValidators"),
        encoder=_encode_validators,
    )

    materializers = _prepare_materializers(
        cel_env=env, materializers=spec.get("materializers")
    )

    outcome = _prepare_outcome(cel_env=env, outcome=spec.get("outcome"))

    loop = asyncio.get_event_loop()
    for workflow_key in get_function_workflows(function=cache_key):
        workflow_task = loop.create_task(
            prepare_workflow(
                cache_key=workflow_key,
                spec=get_spec_from_cache(
                    resource_type=Workflow, cache_key=workflow_key
                ),
            )
        )
        __tasks.add(workflow_task)
        workflow_task.add_done_callback(__tasks.discard)

    return structure.Function(
        managed_resource=managed_resource,
        input_validators=input_validators,
        materializers=materializers,
        outcome=outcome,
        template=spec.get("template"),
    )


def _build_resource_settings(spec: dict | None):
    if not spec:
        return structure.ManagedResource(
            crd=None,
            behaviors=structure.ManagerBehavior(
                virtual=False, load="name", update="patch", delete="destroy"
            ),
        )

    crd_spec = spec.get("crd")
    if not crd_spec:
        crd = None
    else:
        kind = crd_spec.get("kind")
        plural = crd_spec.get("plural")
        crd = structure.ManagedCRD(
            api_version=crd_spec.get("apiVersion"),
            kind=crd_spec.get("kind"),
            plural=plural if plural else f"{kind.lower()}s",
            namespaced=crd_spec.get("namespaced", True),
        )

    behavior_spec = spec.get("behaviors", {})
    behaviors = structure.ManagerBehavior(
        virtual=behavior_spec.get("virtual", False),
        load=behavior_spec.get("load", "name"),
        update=behavior_spec.get("update", "patch"),
        delete=behavior_spec.get("delete", "destroy"),
    )

    return structure.ManagedResource(crd=crd, behaviors=behaviors)


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
            encoder=_encode_validators,
        )

    ok_value = None
    ok_value_spec = outcome.get("okValue")
    if ok_value_spec:
        compiled = cel_env.compile(ok_value_spec)
        ok_value = cel_env.program(compiled)
        ok_value.logger.setLevel(logging.WARNING)

    return structure.Outcome(tests=tests, ok_value=ok_value)


def _template_extractor(
    cel_env: celpy.Environment,
    template_spec: dict | None,
) -> celpy.Runner | None:
    if not template_spec:
        return None

    field_expressions = _encode_template("", template_spec=template_spec)

    materializer = f"{{{','.join([f'"{field}": {expression}'
     for field, expression in field_expressions
     ])}}}"

    compiled = cel_env.compile(materializer)
    program = cel_env.program(compiled)
    program.logger.setLevel(logging.WARNING)
    return program


def _encode_template(base: str, template_spec: dict | None) -> list[tuple[str, Any]]:
    output: list[tuple[str, Any]] = []

    if not template_spec:
        return []

    for field, expression in template_spec.items():
        safe_field = field.replace('"', "'")

        field_name = safe_field
        if base:
            field_name = f"{base}.{safe_field}"

        if isinstance(expression, dict):
            output.extend(_encode_template(field_name, expression))
        else:
            print(
                f"field name: {field_name}, expression (type={type(expression)}): {expression}"
            )
            output.append((field_name, expression))

    return output


def _predicate_extractor(
    cel_env: celpy.Environment,
    predicate_spec: list[dict] | None,
    encoder: Callable[[list], str],
) -> celpy.Runner | None:
    if not predicate_spec:
        return None

    predicates = encoder(predicate_spec)

    tests = f"{predicates}.filter(predicate, predicate.test)"
    compiled = cel_env.compile(tests)

    program = cel_env.program(compiled)
    program.logger.setLevel(logging.WARNING)
    return program


def _encode_validators(predicates: list) -> str:

    output = []

    for predicate in predicates:
        predicate_parts = []

        type_: str = predicate.get("type")
        predicate_parts.append(f'"type": "{type_}"')

        if type_ == "Retry":
            predicate_parts.append(f'"delay": {predicate.get('delay')}')

        message: str = predicate.get("message")
        if message:
            message = message.replace('"', '\"')  # fmt: skip
            predicate_parts.append(f'"message": "{message}"')

        predicate_parts.append(f'"test": {predicate.get('test')}')

        output.append(f"{{{','.join(predicate_parts)}}}")

    return f"[{','.join(output)}]"
