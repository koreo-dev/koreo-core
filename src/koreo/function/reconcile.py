from typing import Any, Dict, List, NamedTuple, Union

import copy
import logging
import json

logger = logging.getLogger("koreo.function")

import jsonpath_ng
import kr8s

import celpy
from celpy import celtypes
from celpy.celparser import tree_dump


from koreo.result import (
    DepSkip,
    Ok,
    Outcome,
    PermFail,
    Retry,
    Skip,
    UnwrappedOutcome,
    is_error,
    is_ok,
    is_not_ok,
    is_unwrapped_ok,
)

from koreo.cel.encoder import convert_bools
from koreo.cache import get_resource_from_cache
from koreo.resource_template.structure import ResourceTemplate

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
    trigger: celtypes.Value,
    inputs: celtypes.Value,
) -> UnwrappedOutcome[celtypes.Value]:
    base_inputs = {"inputs": inputs}

    validation_outcome = _run_checks(
        checks=function.input_validators, inputs=base_inputs, location=location
    )
    if is_not_ok(validation_outcome):
        return validation_outcome

    resource_config = _load_resource_config(
        resource_config=function.resource_config, inputs=base_inputs, location=location
    )
    if not is_unwrapped_ok(resource_config):
        return resource_config

    base_inputs = base_inputs | {
        "inputs": inputs,
        "context": resource_config.context,
    }

    # Create base resource
    managed_resource = _materialize_overlay(
        template=resource_config.base_template,
        materializer=function.materializers.base,
        inputs=base_inputs,
        location=location,
    )

    if not is_unwrapped_ok(managed_resource):
        return managed_resource

    # TODO: Add owners, if behaviors.add-owner
    owner_refs = _managed_owners(managed_resource, trigger)
    if owner_refs:
        managed_resource[celtypes.StringType("metadata")][
            celtypes.StringType("ownerReferences")
        ] = owner_refs

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

    full_inputs = base_inputs | {
        "resource": celpy.json_to_cel(resource) if resource else None
    }

    outcome_validators_outcome = _run_checks(
        checks=function.outcome.validators, inputs=full_inputs, location=location
    )
    if not is_ok(outcome_validators_outcome):
        return outcome_validators_outcome

    if not function.outcome.return_value:
        return celpy.json_to_cel(None)

    try:
        return_value = function.outcome.return_value.evaluate(full_inputs)

        eval_errors = _check_for_celevalerror(return_value)
        if is_not_ok(eval_errors):
            return eval_errors

        return return_value
    except celpy.CELEvalError as err:
        msg = f"CEL Eval Error computing OK value. {err.tree}"
        logger.exception(msg)
        return PermFail(msg, location=location)
    except:
        msg = "Failure computing OK value."
        logger.exception(msg)
        return PermFail(msg, location=location)


def _managed_owners(managed_resource: celtypes.MapType, trigger: celtypes.MapType):
    if not isinstance(managed_resource, celtypes.MapType):
        # This would be non-ideal, and probably shouldn't happen.
        return None

    if "metadata" not in trigger or "metadata" not in managed_resource:
        # No metadata block, which is not great...
        return None

    managed_resource_namespace = managed_resource[celtypes.StringType("metadata")].get(
        celtypes.StringType("namespace"), ""
    )
    trigger_namespace = trigger[celtypes.StringType("metadata")].get(
        celtypes.StringType("namespace"), ""
    )

    if managed_resource_namespace != trigger_namespace:
        # You can only own a resource in the same namespace.
        return None

    trigger_uid = trigger[celtypes.StringType("metadata")].get(
        celtypes.StringType("uid")
    )
    if not trigger_uid:
        # I'm not sure how this would happen. Perhaps in testing?
        return None

    managed_resource_owners: celtypes.ListType = copy.deepcopy(
        managed_resource[celtypes.StringType("metadata")].get(
            celtypes.StringType("ownerReferences"), celtypes.ListType()
        )
    )
    if not managed_resource_owners:
        return celtypes.ListType([trigger.get(celtypes.StringType("ownerRef"))])

    for owner in managed_resource_owners:
        if owner.get("uid") == trigger_uid:
            return None

    managed_resource_owners.append(trigger.get(celtypes.StringType("ownerRef")))

    return managed_resource_owners


def _check_for_celevalerror(value: celtypes.Value) -> Outcome[None]:
    if isinstance(value, celpy.CELEvalError):
        tree = tree_dump(value.tree) if value and value.tree else ""
        return PermFail(message=f"CELEvalError (at {tree}) {value.args}", location=tree)

    if isinstance(value, celtypes.MapType):
        for key, subvalue in value.items():
            key_ok = _check_for_celevalerror(key)
            if is_not_ok(key_ok):
                return key_ok

            subvalue_ok = _check_for_celevalerror(subvalue)
            if is_not_ok(subvalue_ok):
                return subvalue_ok

    if isinstance(value, celtypes.ListType):
        for subvalue in value:
            subvalue_ok = _check_for_celevalerror(subvalue)
            if is_not_ok(subvalue_ok):
                return subvalue_ok

    return Ok(None)


class _ResourceConfig(NamedTuple):
    behavior: Behavior
    managed_resource: ManagedResource
    base_template: celtypes.MapType
    context: celtypes.MapType


def _load_resource_config(
    resource_config: StaticResource | DynamicResource | None,
    inputs: dict[str, celtypes.Value],
    location: str,
) -> UnwrappedOutcome[_ResourceConfig]:
    match resource_config:
        case StaticResource(
            behavior=behavior, managed_resource=managed_resource, context=context
        ):
            return _ResourceConfig(
                behavior=behavior,
                managed_resource=managed_resource,
                base_template=celtypes.MapType(),
                context=context,
            )

        case DynamicResource(key=key):
            template_key = key.evaluate(inputs)
            resource_template = get_resource_from_cache(
                resource_class=ResourceTemplate, cache_key=f"{template_key}"
            )
            if not resource_template:
                return Retry(
                    message=f'ResourceTemplate ("{template_key}") not found, will retry.',
                    delay=60,
                    location=location,
                )

            if is_error(resource_template):
                return Retry(
                    message=f'ResourceTemplate ("{template_key}") requires correction ({resource_template.message}). Will retry while waiting.',
                    delay=180,
                    location=location,
                )

            return _ResourceConfig(
                behavior=resource_template.behavior,
                managed_resource=resource_template.managed_resource,
                base_template=resource_template.template,
                context=resource_template.context,
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

    try:
        return _predicate_to_koreo_result(checks.evaluate(inputs), location=location)
    except Exception as err:
        return PermFail(
            f"Error evaluating checks for {location}. {err}", location=location
        )


def _predicate_to_koreo_result(results: list, location: str) -> Outcome:
    if not results:
        return Ok(None)

    for result in results:
        match result:
            case {"assert": _, "ok": {}}:
                return Ok(None, location=location)

            case {"assert": _, "depSkip": {"message": message}}:
                return DepSkip(message=message, location=location)

            case {"assert": _, "skip": {"message": message}}:
                return Skip(message=message, location=location)

            case {"assert": _, "retry": {"message": message, "delay": delay}}:
                return Retry(
                    message=message,
                    delay=delay,
                    location=location,
                )

            case {"assert": _, "permFail": {"message": message}}:
                return PermFail(message=message, location=location)

            case _:
                return PermFail(f"Unknown predicate type: {result}", location=location)

    return Ok(None)


def _materialize_overlay(
    template: celtypes.MapType | None,
    materializer: celpy.Runner | None,
    inputs: dict[str, celtypes.Value],
    location: str,
) -> UnwrappedOutcome[celtypes.MapType]:
    managed_resource = copy.deepcopy(template) if template else celtypes.MapType()

    if not materializer:
        return managed_resource

    try:
        computed_inputs = inputs | {"template": managed_resource}

        overlay = materializer.evaluate(computed_inputs)

        eval_errors = _check_for_celevalerror(overlay)
        if is_not_ok(eval_errors):
            return eval_errors

    except celpy.CELEvalError as err:
        return PermFail(
            f"Encountered CELEvalError {err}. ({location})", location=location
        )

    except TypeError as err:
        return PermFail(f"Encountered TypeError {err}. ({location})", location=location)

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
    managed_resource: celtypes.MapType,
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
        or resource_api_params.kind
        or resource_api_params.name
    ):
        return PermFail(
            "To compute a retun value, use `spec.outcome.return`. For resources, "
            f"`kind` ({resource_api_params.kind}), `apiVersion` ({resource_api_params.api_version}), "
            f"and `metadata.name` ({resource_api_params.name}) are all required.",
            location=location,
        )
    elif not (
        resource_api_params.api_version
        and resource_api_params.kind
        and resource_api_params.name
    ):
        return PermFail(
            f"`kind` ({resource_api_params.kind}), `apiVersion` ({resource_api_params.api_version}), "
            f"and `metadata.name` ({resource_api_params.name}) are all required. "
            f"({json.dumps(managed_resource)}, {managed_resource_config}, {resource_api_params._asdict()})",
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
            logger.exception(msg)
            return Retry(message=msg, delay=30, location=location)
    except Exception as err:
        msg = f"Failure loading {resource_api} resource {resource_name}. ({type(err)}: {err})"
        logger.exception(msg)
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

        if not is_unwrapped_ok(managed_resource):
            return managed_resource

        new_object = resource_class(
            api=api,
            resource=convert_bools(managed_resource),
            namespace=resource_api_params.namespace,
        )

        try:
            await new_object.create()
        except TypeError as err:
            logger.error(f"Error creating: {managed_resource}")
            # TODO: Should this be Retry or PermFail? What about timeouts /
            # transient errors?
            return PermFail(
                message=f"Error creating {resource_api} resource {resource_name}: {err}",
                location=location,
            )

        # TODO: Dynamic create delay?
        return Retry(
            message=f"Creating {resource_api} resource {resource_name}.",
            delay=30,
            location=location,
        )

    if not resource:
        return None

    py_managed_resource = convert_bools(managed_resource)
    if _validate_match(py_managed_resource, resource.raw):
        logger.debug(
            f"{resource_api}/{resource_name} resource matched spec, skipping update. ({location})"
        )

        return resource.raw

    logger.info(
        f"{resource_api}/{resource_name} resource did not match spec. ({location})"
    )
    if behavior.update == "patch":
        # TODO: Should this only send fields with differences?
        logger.info(
            f"Patching {resource_api}/{resource_name} to match spec. ({location})"
        )
        await resource.patch(py_managed_resource)
        return Retry(
            message=f"Updating {resource_api} resource {resource_name} to match template.",
            delay=5,
            location=location,
        )
    elif behavior.update == "recreate":
        logger.info(
            f"Will re-create {resource_api}/{resource_name} to match spec. ({location})"
        )
        await resource.delete()
        return Retry(
            message=f"Deleting {resource_api} resource {resource_name} to recreate.",
            delay=5,
            location=location,
        )
    else:
        logger.info(
            f"Skipping update (behavior.update is 'never') for {resource_api}/{resource_name}. ({location})"
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
        if target_key == "ownerReferences":
            continue

        if target_key not in actual:
            return False

        if not _validate_match(target[target_key], actual[target_key]):
            return False

    return True


def _validate_list_match(target: list | tuple, actual: list | tuple) -> bool:
    if len(target) != len(actual):
        return False

    for target_value, actual_value in zip(target, actual):
        if not _validate_match(target_value, actual_value):
            return False

    return True
