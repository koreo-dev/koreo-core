from typing import Any, NamedTuple

import logging

import copy
import json

import kr8s
import jsonpath_ng

import celpy
from celpy import celtypes

from koreo.result import (
    DepSkip,
    Ok,
    Outcome,
    PermFail,
    Retry,
    Skip,
    combine,
    is_error,
    is_ok,
)

from koreo.resource_template.registry import get_resource_template

from .structure import (
    Behavior,
    DynamicResource,
    Function,
    ManagedResource,
    StaticResource,
)


async def reconcile_function(
    api: kr8s.Api,
    location: str,
    function: Function,
    trigger: dict,
    inputs: dict,
):
    if not is_ok(function.function_ready):
        return function.function_ready

    base_inputs = {
        "inputs": celpy.json_to_cel(inputs),
        "parent": celpy.json_to_cel(trigger),
    }

    validation_outcome = _run_checks(
        checks=function.input_validators, inputs=base_inputs, location=location
    )
    if not is_ok(validation_outcome):
        return validation_outcome

    resource_config = _load_resource_config(
        resource_config=function.resource_config, inputs=base_inputs, location=location
    )
    if is_error(resource_config):
        return resource_config

    # Create base resource
    managed_resource = _materialize_overlay(
        template=resource_config.base_template,
        materializer=function.materializers.base,
        inputs=base_inputs,
        location=location,
    )

    # TODO: Add owners, if behaviors.add-owner

    resource = await _resource_crud(
        api=api,
        location=location,
        behavior=resource_config.behavior,
        managed_resource=managed_resource,
        managed_resource_config=resource_config.managed_resource,
        on_create=function.materializers.on_create,
        inputs=base_inputs,
    )
    if is_error(resource):
        return resource

    inputs = base_inputs | {
        "resource": celpy.json_to_cel(resource) if resource else None
    }

    outcome_tests_outcome = _run_checks(
        checks=function.outcome.tests, inputs=inputs, location=location
    )
    if not is_ok(outcome_tests_outcome):
        return outcome_tests_outcome

    if not function.outcome.ok_value:
        return Ok(None)

    try:
        ok_value = function.outcome.ok_value.evaluate(inputs)
        return Ok(json.loads(json.dumps(ok_value)), location=location)
    except celpy.CELEvalError as err:
        msg = f"CEL Eval Error computing OK value. {err.tree}"
        logging.exception(msg)
        return PermFail(msg, location=location)
    except:
        msg = "Failure computing OK value."
        logging.exception(msg)
        return PermFail(msg, location=location)


class _ResourceConfig(NamedTuple):
    behavior: Behavior
    managed_resource: ManagedResource
    base_template: dict


def _load_resource_config(
    resource_config: StaticResource | DynamicResource | None,
    inputs: dict[str, celtypes.Value],
    location: str,
):
    match resource_config:
        case StaticResource(behavior=behavior, managed_resource=managed_resource):
            return _ResourceConfig(
                behavior=behavior, managed_resource=managed_resource, base_template={}
            )

        case DynamicResource(key=key):
            template_key = json.loads(json.dumps(key.evaluate(inputs)))
            resource_template = get_resource_template(template_key=template_key)
            if not resource_template:
                return Retry(
                    message=f'ResourceTemplate ("{template_key}") not found, will retry.',
                    delay=60,
                    location=location,
                )

            return _ResourceConfig(
                behavior=resource_template.behavior,
                managed_resource=resource_template.managed_resource,
                base_template=resource_template.template,
            )

    return PermFail(
        "Must specify either dynamicResource or staticResource config.",
        location=location,
    )


def _run_checks(
    checks: celpy.Runner | None, inputs: dict[str, celtypes.Value], location: str
):
    if not checks:
        return Ok(None, location=location)

    check_results = json.loads(json.dumps(checks.evaluate(inputs)))
    return _predicate_to_koreo_result(check_results, location=location)


def _predicate_to_koreo_result(results: list, location: str) -> Outcome:
    outcomes = []

    for result in results:
        match result.get("type"):
            case "DepSkip":
                outcomes.append(
                    DepSkip(message=result.get("message"), location=location)
                )
            case "Ok":
                outcomes.append(Ok(None, location=location))
            case "PermFail":
                outcomes.append(
                    PermFail(message=result.get("message"), location=location)
                )
            case "Retry":
                outcomes.append(
                    Retry(
                        message=result.get("message"),
                        delay=result.get("delay"),
                        location=location,
                    )
                )
            case "Skip":
                outcomes.append(Skip(message=result.get("message"), location=location))
            case _ as t:
                outcomes.append(
                    PermFail(f"Unknown predicate result type: {t}", location=location)
                )

    if not outcomes:
        return Ok(None)

    return combine(outcomes=outcomes)


def _materialize_overlay(
    template: dict[str, Any] | None,
    materializer: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
    location: str,
):
    managed_resource = copy.deepcopy(template) if template else {}

    if not materializer:
        return managed_resource

    try:
        computed = materializer.evaluate(
            inputs
            | {
                "template": celpy.json_to_cel(managed_resource),
            }
        )
        overlay = json.loads(json.dumps(computed))
    except celpy.CELEvalError:
        logging.exception(f"Encountered CELEvalError {computed}. ({location})")

        raise

    if overlay:
        for field_path, value in overlay.items():
            field_expr = jsonpath_ng.parse(field_path)
            field_expr.update_or_create(managed_resource, value)

    return managed_resource


class _Resource(NamedTuple):
    api_version: str | None
    kind: str | None
    plural: str | None
    namespaced: bool
    name: str | None
    namespace: str | None


def _build_resource_config(
    managed_resource: dict,
    managed_resource_config: ManagedResource,
):
    computed_metadata = managed_resource.get("metadata", {})

    name = computed_metadata.get("name")
    namespace = computed_metadata.get("namespace")

    if managed_resource_config:
        api_version = managed_resource_config.api_version
        kind = managed_resource_config.kind
        plural = managed_resource_config.plural
        namespaced = managed_resource_config.namespaced
    else:
        api_version = managed_resource.get("apiVersion")
        kind = managed_resource.get("kind")
        plural = None
        namespaced = True if namespace else False

    return _Resource(
        api_version=api_version,
        kind=kind,
        plural=plural,
        namespaced=namespaced,
        name=name,
        namespace=namespace,
    )


async def _resource_crud(
    api: kr8s.Api,
    location: str,
    behavior: Behavior,
    managed_resource: dict,
    managed_resource_config: ManagedResource,
    on_create: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
):
    if not managed_resource:
        return None

    resource_api_params = _build_resource_config(
        managed_resource=managed_resource,
        managed_resource_config=managed_resource_config,
    )

    if not (
        resource_api_params.api_version
        and resource_api_params.kind
        and resource_api_params.name
    ):
        return PermFail(
            f"kind ({resource_api_params.kind}), apiVersion ({resource_api_params.api_version}), and metadata.name ({resource_api_params.name}) are all required. ({managed_resource}, {managed_resource_config}, {resource_api_params._asdict()})",
            location=location,
        )

    resource_class = kr8s.objects.new_class(
        version=resource_api_params.api_version,
        kind=resource_api_params.kind,
        plural=resource_api_params.plural,
        namespaced=resource_api_params.namespaced,
        asyncio=True,
    )

    resource_api = f"{resource_api_params.kind}.{resource_api_params.api_version}"
    resource_name = f"{resource_api_params.namespace}/{resource_api_params.name}"

    try:
        resource_matches = await api.async_get(
            resource_class,
            resource_api_params.name,
            namespace=resource_api_params.namespace,
        )
    except kr8s.NotFoundError:
        resource_matches = None
    except kr8s.ServerError as err:
        if err.response and err.response.status_code == 404:
            resource_matches = None
        else:
            msg = f"ServerError loading {resource_api} resource {resource_name}. ({type(err)}: {err})"
            logging.exception(msg)
            return Retry(message=msg, delay=30, location=location)
    except Exception as err:
        msg = f"Failure loading {resource_api} resource {resource_name}. ({type(err)}: {err})"
        logging.exception(msg)
        return Retry(message=msg, delay=30, location=location)

    if not resource_matches:
        resource = None
    else:
        if len(resource_matches) > 1:
            return PermFail(
                f"{resource_api}/{resource_name} resource matched multiple resources."
            )
        else:
            resource = resource_matches[0]

    if not resource and behavior.create:
        managed_resource = _materialize_overlay(
            template=managed_resource,
            materializer=on_create,
            inputs=inputs,
            location=location,
        )

        logging.info(f"Creating {resource_api} resource {resource_name}. ({location})")
        new_object = resource_class(
            api=api, resource=managed_resource, namespace=resource_api_params.namespace
        )
        await new_object.create()
        # TODO: Dynamic create delay?
        return Retry(
            message=f"Creating {resource_api} resource {resource_name}.",
            delay=30,
            location=location,
        )

    if not resource:
        return None

    if _validate_match(managed_resource, resource.raw):
        logging.debug(
            f"{resource_api}/{resource_name} resource matched spec, skipping update. ({location})"
        )
        return resource.raw

    logging.info(
        f"{resource_api}/{resource_name} resource did not match spec, updating. ({location})"
    )
    if behavior.update == "patch":
        # TODO: Should this only send fields with differences?
        await resource.patch(managed_resource)
        return Retry(
            message=f"Updating {resource_api} resource {resource_name} to match template.",
            delay=5,
            location=location,
        )
    elif behavior.update == "recreate":
        await resource.delete()
        return Retry(
            message=f"Deleting {resource_api} resource {resource_name} to recreate.",
            delay=5,
            location=location,
        )

    return resource.raw


def _validate_match(target, actual):
    """Compare the specified (`target`) state against the actual (`actual`)
    reosurce state. We compare all target fields and ignore anything extra.
    """
    if isinstance(target, dict) and isinstance(actual, dict):
        return _validate_dict_match(target, actual)

    if isinstance(target, (list, tuple)) and isinstance(actual, (list, tuple)):
        return _validate_list_match(target, actual)

    return target == actual


def _validate_dict_match(target: dict, actual: dict) -> bool:
    for target_key in target.keys():
        if target_key not in actual:
            return False

        if not _validate_match(target[target_key], actual[target_key]):
            return False

    return True


def _validate_list_match(target: list | tuple, actual: list | tuple) -> bool:
    if len(target) != len(actual):
        return False

    for target_value, source_value in zip(target, actual):
        if not _validate_match(target_value, source_value):
            return False

    return True
