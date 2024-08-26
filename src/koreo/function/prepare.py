from typing import Any, Callable
import logging

from . import structure

import celpy
import celpy.celtypes as celtypes


def prepare_function(function: dict) -> structure.Function:
    # NOTE: We can try `celpy.Environment(runner_class=celpy.CompiledRunner)`
    # We need to do a safety check to ensure there are no escapes / injections.

    env = celpy.Environment()

    input_validators = _predicate_extractor(
        cel_env=env,
        predicate_spec=function.get("inputValidators"),
        encoder=_encode_validators,
    )

    materializers = _prepare_materializers(
        cel_env=env, materializers=function.get("materializers")
    )

    outcome = _prepare_outcome(cel_env=env, outcome=function.get("outcome"))

    return structure.Function(
        input_validators=input_validators,
        materializers=materializers,
        outcome=outcome,
        template=function.get("template"),
    )


def _prepare_materializers(
    cel_env: celpy.Environment, materializers: dict
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


def _prepare_outcome(cel_env: celpy.Environment, outcome: dict) -> structure.Outcome:
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
