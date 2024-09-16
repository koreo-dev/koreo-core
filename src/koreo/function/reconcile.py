from typing import Any, NamedTuple

import logging

import copy
import json

import kr8s
import jsonpath_ng

import celpy
from celpy import celtypes

from ..result import (
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

from .structure import Function, ManagedResource


async def reconcile_function(
    api: kr8s.Api,
    function: Function,
    trigger: dict,
    inputs: dict,
):
    base_inputs = {
        "inputs": celpy.json_to_cel(inputs),
        "parent": celpy.json_to_cel(trigger),
    }

    validation_outcome = _run_checks(
        checks=function.input_validators,
        inputs=base_inputs,
    )
    if not is_ok(validation_outcome):
        return validation_outcome

    # Create base resource
    managed_resource = _materialize_overlay(
        template=function.template,
        materializer=function.materializers.base,
        inputs=base_inputs,
    )

    # TODO: Add owners, if ManagedResource.behaviors.add-owner

    resource = await _resource_crud(
        api=api,
        managed_resource=managed_resource,
        resource_config=function.managed_resource,
        on_create=function.materializers.on_create,
        inputs=base_inputs,
    )
    if is_error(resource):
        return resource

    inputs = base_inputs | {
        "resource": celpy.json_to_cel(resource) if resource else None
    }

    outcome_tests_outcome = _run_checks(checks=function.outcome.tests, inputs=inputs)
    if not is_ok(outcome_tests_outcome):
        return outcome_tests_outcome

    if not function.outcome.ok_value:
        return Ok(None)

    try:
        ok_value = function.outcome.ok_value.evaluate(inputs)
        return Ok(json.loads(json.dumps(ok_value)))
    except celpy.CELEvalError as err:
        msg = f"CEL Eval Error computing OK value. {err.tree}"
        logging.exception(msg)
        return PermFail(msg)
    except:
        msg = "Failure computing OK value."
        logging.exception(msg)
        return PermFail(msg)


def _run_checks(checks: celpy.Runner | None, inputs: dict[str, celtypes.Value]):
    if not checks:
        return Ok(None)

    check_results = json.loads(json.dumps(checks.evaluate(inputs)))
    return _predicate_to_koreo_result(check_results)


def _predicate_to_koreo_result(results: list) -> Outcome:
    outcomes = []

    for result in results:
        match result.get("type"):
            case "DepSkip":
                outcomes.append(DepSkip(message=result.get("message")))
            case "Ok":
                outcomes.append(Ok(None))
            case "PermFail":
                outcomes.append(PermFail(message=result.get("message")))
            case "Retry":
                outcomes.append(
                    Retry(message=result.get("message"), delay=result.get("delay"))
                )
            case "Skip":
                outcomes.append(Skip(message=result.get("message")))
            case _ as t:
                outcomes.append(PermFail(f"Unknown predicate result type: {t}"))

    if not outcomes:
        return Ok(None)

    return combine(outcomes=outcomes)


def _materialize_overlay(
    template: dict[str, Any] | None,
    materializer: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
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
        logging.exception(f"Encountered CELEvalError {computed}")

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
    resource_config: ManagedResource,
):
    computed_metadata = managed_resource.get("metadata", {})

    name = computed_metadata.get("name")
    namespace = computed_metadata.get("namespace")

    if resource_config.crd:
        api_version = resource_config.crd.api_version
        kind = resource_config.crd.kind
        plural = resource_config.crd.plural
        namespaced = resource_config.crd.namespaced
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
    managed_resource: dict,
    resource_config: ManagedResource,
    on_create: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
):
    if not managed_resource:
        return None

    resource_params = _build_resource_config(
        managed_resource=managed_resource, resource_config=resource_config
    )

    if not (
        resource_params.api_version and resource_params.kind and resource_params.name
    ):
        return PermFail(
            f"kind ({resource_params.kind}), apiVersion ({resource_params.api_version}), and metadata.name ({resource_params.name}) are all required."
        )

    resource_class = kr8s.objects.new_class(
        version=resource_params.api_version,
        kind=resource_params.kind,
        plural=resource_params.plural,
        namespaced=resource_params.namespaced,
        asyncio=True,
    )

    try:
        resource_matches = await api.async_get(
            resource_class,
            resource_params.name,
            namespace=resource_params.namespace,
        )
    except kr8s.NotFoundError:
        resource_matches = None
    except kr8s.ServerError as err:
        if err.response and err.response.status_code == 404:
            resource_matches = None
        else:
            msg = f"ServerError loading {resource_params.kind} resource {resource_params.namespace}/{resource_params.name}. ({type(err)}: {err})"
            logging.exception(msg)
            return Retry(message=msg, delay=30)
    except Exception as err:
        msg = f"Failure loading {resource_params.kind} resource {resource_params.namespace}/{resource_params.name}. ({type(err)}: {err})"
        logging.exception(msg)
        return Retry(message=msg, delay=30)

    if not resource_matches:
        resource = None
    else:
        if len(resource_matches) > 1:
            return PermFail(
                f"{resource_params.kind}.{resource_params.api_version}/{resource_params.name} resource matched multiple resources."
            )
        else:
            resource = resource_matches[0]

    if not resource and resource_config.behaviors.create:
        managed_resource = _materialize_overlay(
            template=managed_resource,
            materializer=on_create,
            inputs=inputs,
        )

        logging.info(
            f"Creating {resource_params.kind}.{resource_params.api_version} resource {resource_params.namespace}/{resource_params.name}."
        )
        new_object = resource_class(
            api=api, resource=managed_resource, namespace=resource_params.namespace
        )
        await new_object.create()
        # TODO: Dynamic create delay?
        return Retry(
            message=f"Creating {resource_params.kind}.{resource_params.api_version} resource {resource_params.namespace}/{resource_params.name}.",
            delay=30,
        )

    if not resource:
        return None

    if _validate_match(managed_resource, resource.raw):
        logging.debug(
            f"{resource_params.kind}.{resource_params.api_version}/{resource_params.name} resource matched spec, skipping update."
        )
        return resource.raw

    logging.info(
        f"{resource_params.kind}.{resource_params.api_version}/{resource_params.name} resource did not match spec, updating."
    )
    if resource_config.behaviors.update == "patch":
        # TODO: Should this only send fields with differences?
        await resource.patch(managed_resource)
        return Retry(
            message=f"Updating {resource_params.kind}.{resource_params.api_version} resource {resource_params.namespace}/{resource_params.name} to match template.",
            delay=5,
        )
    elif resource_config.behaviors.update == "recreate":
        await resource.delete()
        return Retry(
            message=f"Deleting {resource_params.kind}.{resource_params.api_version} resource {resource_params.namespace}/{resource_params.name} to recreate.",
            delay=5,
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
