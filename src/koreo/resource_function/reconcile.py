from typing import NamedTuple, Sequence
import copy
import logging

logger = logging.getLogger("koreo.function")

from kr8s._objects import APIObject
import kr8s

import celpy
from celpy import celtypes

from koreo import cache
from koreo.cel import functions
from koreo.cel.encoder import convert_bools
from koreo.cel.evaluation import (
    evaluate,
    evaluate_overlay,
    evaluate_predicates,
    check_for_celevalerror,
)
from koreo.resource_template.structure import ResourceTemplate
from koreo.result import PermFail, Retry, UnwrappedOutcome, is_unwrapped_ok


from . import structure

DEFAULT_LOAD_RETRY_DELAY = 30


class Result(NamedTuple):
    outcome: UnwrappedOutcome[celtypes.Value]
    resource_id: dict | None = None


async def reconcile_resource_function(
    api: kr8s.Api,
    location: str,
    function: structure.ResourceFunction,
    owner: tuple[str, dict],
    inputs: celtypes.Value,
) -> Result:
    full_inputs: dict[str, celtypes.Value] = {
        "inputs": inputs,
    }

    if validator_error := evaluate_predicates(
        predicates=function.input_validators,
        inputs=full_inputs,
        location="spec.inputValidators",
    ):
        return Result(outcome=validator_error)

    match evaluate(
        expression=function.local_values, inputs=full_inputs, location="spec.locals"
    ):
        case PermFail(message=message, location=locals_location):
            return Result(
                outcome=PermFail(
                    message=message,
                    location=(
                        locals_location if locals_location else f"{location}.locals"
                    ),
                )
            )
        case None:
            full_inputs["locals"] = celtypes.MapType({})
        case celtypes.MapType() as local_values:
            full_inputs["locals"] = local_values
        case bad_type:
            # Due to validation within `prepare`, this should never happen.
            return Result(
                outcome=PermFail(
                    message=f"Invalid `locals` expression type ({type(bad_type)})",
                    location=f"{location}.locals",
                )
            )

    ###########################
    # Start Kubernetes Specific
    ###########################

    reconcile_result = await reconcile_krm_resource(
        api=api,
        crud_config=function.crud_config,
        owner=owner,
        inputs=full_inputs,
    )
    if not is_unwrapped_ok(reconcile_result.result):
        return Result(
            outcome=reconcile_result.result, resource_id=reconcile_result.resource_id
        )

    full_inputs["resource"] = celpy.json_to_cel(reconcile_result.result)

    #########################
    # End Kubernetes Specific
    #########################

    return Result(
        outcome=_reconcile_outcome(
            outcome=function.outcome, inputs=full_inputs, name=location
        ),
        resource_id=reconcile_result.resource_id,
    )


def _reconcile_outcome(
    outcome: structure.Outcome, inputs: dict[str, celtypes.Value], name: str
):
    if err := evaluate_predicates(
        predicates=outcome.validators,
        inputs=inputs,
        location=f"{name}:spec.outcome.validators",
    ):
        return err

    return evaluate(
        outcome.return_value, inputs=inputs, location=f"{name}:spec.outcome.return"
    )


class ReconcileResult(NamedTuple):
    result: PermFail | Retry | dict
    resource_id: dict | None = None


async def reconcile_krm_resource(
    api: kr8s.Api,
    crud_config: structure.CRUDConfig,
    owner: tuple[str, dict],
    inputs: dict[str, celtypes.Value],
) -> ReconcileResult:
    match evaluate(
        expression=crud_config.resource_id,
        inputs=inputs,
        location="spec.apiConfig.name",
    ):
        case PermFail(message=message, location=name_location):
            return ReconcileResult(
                result=PermFail(
                    message=message,
                    location=(
                        name_location if name_location else f"spec.apiConfig.name"
                    ),
                )
            )

        case None:
            return ReconcileResult(
                result=PermFail(
                    message="Could not evaluate `spec.apiConfig.name`, evaluated to `null`",
                    location=f"spec.apiConfig.name",
                )
            )

        case celtypes.MapType({"name": name_value}) as resource_name_values:
            name = f"{name_value}"
            namespace_value = resource_name_values.get("namespace")
            if namespace_value:
                namespace = f"{namespace_value}"
            else:
                namespace = None

            if not namespace and crud_config.resource_api.namespaced:
                return ReconcileResult(
                    result=PermFail(
                        message="`namespace` is required when `spec.apiConfig.namespaced` is `true`",
                        location=f"spec.apiConfig.namespace",
                    )
                )

        case bad_type:
            # Due to validation within `prepare`, this should never happen.
            return ReconcileResult(
                result=PermFail(
                    message=f"Invalid `spec.apiConfig.name` expression type ({type(bad_type)})",
                    location=f"spec.apiConfig.name",
                )
            )

    resource_id = {
        "apiVersion": crud_config.resource_api.version,
        "kind": crud_config.resource_api.kind,
        "plural": crud_config.resource_api.plural,
        "name": name,
        "readonly": crud_config.readonly,
    }
    if namespace:
        full_resource_name = f"{crud_config.resource_api.kind}:{namespace}:{name}"
        resource_id["namespace"] = namespace
    else:
        full_resource_name = f"{crud_config.resource_api.kind}:{name}"

    api_resource = await load_api_resource(
        api=api,
        resource_api=crud_config.resource_api,
        name=name,
        namespace=namespace,
    )
    if not is_unwrapped_ok(api_resource):
        return ReconcileResult(result=api_resource, resource_id=resource_id)

    if not api_resource and (crud_config.readonly or not crud_config.create.enabled):
        # This is where we used virtual, but I think ValueFunction replaces that?
        # TODO: Return None or return base?
        return ReconcileResult(
            result=Retry(
                message=f"{full_resource_name} not found. Waiting...",
                delay=DEFAULT_LOAD_RETRY_DELAY,
            ),
            resource_id=resource_id,
        )

    if api_resource and crud_config.readonly:
        return ReconcileResult(result=api_resource.raw, resource_id=resource_id)

    forced_overlay = _forced_overlay(
        resource_api=crud_config.resource_api, name=name, namespace=namespace
    )
    if not is_unwrapped_ok(forced_overlay):
        return ReconcileResult(
            result=forced_overlay,
            resource_id=resource_id,
        )

    expected_resource = await _construct_resource_template(
        resource_template=crud_config.resource_template,
        inputs=inputs,
        forced_overlay=forced_overlay,
    )
    if not is_unwrapped_ok(expected_resource):
        return ReconcileResult(
            result=expected_resource,
            resource_id=resource_id,
        )

    if not api_resource:
        return ReconcileResult(
            result=await _create_api_resource(
                api=api,
                resource_api=crud_config.resource_api,
                create=crud_config.create,
                namespace=namespace,
                owned_resource=crud_config.own_resource,
                owner=owner,
                inputs=inputs,
                resource_view=expected_resource,
                forced_overlay=forced_overlay,
                full_resource_name=full_resource_name,
            ),
            resource_id=resource_id,
        )

    owner_namespace, owner_ref = owner
    should_own = crud_config.own_resource and owner_namespace == namespace
    if should_own:
        owner_reffed = _validate_owner_reffed(api_resource.raw, owner_ref)
    else:
        owner_reffed = True

    converted_resource = convert_bools(expected_resource)

    resource_match = _validate_match(converted_resource, api_resource.raw)
    if resource_match.match and owner_reffed:
        logger.debug(f"{full_resource_name} matched spec, no update required.")

        return ReconcileResult(result=api_resource.raw, resource_id=resource_id)

    match crud_config.update:
        case structure.UpdateNever():
            logger.debug(
                f"Skipping update (behavior.update is 'never') for {full_resource_name}."
            )
            return ReconcileResult(result=api_resource.raw, resource_id=resource_id)

        case structure.UpdateRecreate(delay=delay):
            await api_resource.delete()
            return ReconcileResult(
                result=Retry(
                    message=(
                        f"Deleting {full_resource_name} to recreate due to "
                        f"{' -> '.join(resource_match.differences)}."
                    ),
                    delay=delay,
                    location="spec.update.recreate",
                ),
                resource_id=resource_id,
            )

        case structure.UpdatePatch(delay=delay):
            if should_own and not owner_reffed:
                owner_refs = _updated_owner_refs(api_resource.raw, owner_ref)
                if not is_unwrapped_ok(owner_refs):
                    return ReconcileResult(result=owner_refs, resource_id=resource_id)

                converted_resource["metadata"]["ownerReferences"] = owner_refs

            await api_resource.patch(converted_resource)
            return ReconcileResult(
                result=Retry(
                    message=(
                        f"Patching `{full_resource_name}` due to "
                        f"{' -> '.join(resource_match.differences)}."
                    ),
                    delay=delay,
                    location="spec.update.patch",
                ),
                resource_id=resource_id,
            )


async def load_api_resource(
    api: kr8s.Api,
    resource_api: type[APIObject],
    name: str,
    namespace: str | None,
):
    try:
        matches = await api.async_get(
            resource_api,
            name,
            namespace=namespace,
        )
    except kr8s.NotFoundError:
        matches = None
    except kr8s.ServerError as err:
        if not err.response or err.response.status_code != 404:
            msg = (
                f"ServerError loading {resource_api.kind} {name}. ({type(err)}: {err})"
            )
            logger.exception(msg)

            return Retry(
                message=msg, delay=DEFAULT_LOAD_RETRY_DELAY, location="load resource"
            )

        matches = None

    except Exception as err:
        msg = f"Failure loading {resource_api.kind} {name}. ({type(err)}: {err})"
        logger.exception(msg)
        return Retry(
            message=msg, delay=DEFAULT_LOAD_RETRY_DELAY, location="load resource"
        )

    match matches:
        case list() if len(matches) == 1:
            return matches[0]

        case APIObject():
            return matches

        case list() if len(matches) > 1:
            return Retry(
                message=f"{resource_api.kind}/{name} resource matched multiple resources.",
                delay=DEFAULT_LOAD_RETRY_DELAY,
            )

        case list():
            return None


async def _construct_resource_template(
    inputs: dict[str, celtypes.Value],
    resource_template: (
        structure.InlineResourceTemplate | structure.ResourceTemplateRef | None
    ),
    forced_overlay: celtypes.MapType,
):
    match resource_template:
        case None:
            return forced_overlay

        case structure.ResourceTemplateRef(name=template_name, overlay=overlay):
            match evaluate(
                expression=template_name,
                inputs=inputs,
                location="spec.resourceTemplateRef.name",
            ):
                case PermFail(message=message, location=err_location):
                    return PermFail(
                        message=message,
                        location=(
                            err_location
                            if err_location
                            else "spec.resourceTemplateRef.name"
                        ),
                    )
                case celtypes.StringType() as template_key:
                    pass
                case _ as bad_type:
                    return PermFail(
                        message=f"Expected string, but received {type(bad_type)} at `spec.resourceTemplateRef.name`",
                        location=f"spec.resourceTemplateRef.name",
                    )

            dynamic_resource_template = cache.get_resource_from_cache(
                resource_class=ResourceTemplate, cache_key=template_key
            )

            if not dynamic_resource_template:
                return Retry(
                    message=f"ResourceTemplate:{template_key} not found.",
                    location=f"spec.resourceTemplateRef.name:<load>",
                    delay=DEFAULT_LOAD_RETRY_DELAY,
                )

            if not is_unwrapped_ok(dynamic_resource_template):
                return Retry(
                    message=f"ResourceTemplate:{template_key} is not ready ({dynamic_resource_template.message})",
                    location=f"spec.resourceTemplateRef.name:<load>",
                    delay=DEFAULT_LOAD_RETRY_DELAY,
                )

            if not overlay:
                materialized = dynamic_resource_template.template
            else:
                match evaluate_overlay(
                    overlay=overlay,
                    inputs=inputs,
                    base=dynamic_resource_template.template,
                    location="spec.resourceTemplateRef.overlay",
                ):
                    case PermFail(message=message, location=err_location):
                        return PermFail(
                            message=message,
                            location=(
                                err_location
                                if err_location
                                else "spec.resourceTemplateRef.overlay"
                            ),
                        )
                    case celtypes.MapType() as materialized:
                        # Just needed materialized
                        pass
                    case _ as bad_type:
                        return PermFail(
                            message=f"Expected mapping, but received {type(bad_type)} for 'spec.resourceTemplateRef.overlay'",
                            location="spec.resourceTemplateRef.overlay",
                        )

                if err := check_for_celevalerror(
                    materialized, location="spec.resourceTemplateRef.overlay<apply>"
                ):
                    return err

                assert not isinstance(materialized, celpy.CELEvalError)

        case structure.InlineResourceTemplate(template=template):
            match evaluate(
                expression=template,
                inputs=inputs,
                location="spec.resource",
            ):
                case PermFail(message=message, location=err_location):
                    return PermFail(
                        message=message,
                        location=(err_location if err_location else "spec.resource"),
                    )
                case celtypes.MapType() as materialized:
                    pass
                case _ as bad_type:
                    return PermFail(
                        message=f"Expected mapping, but received {type(bad_type)} for `spec.resource`",
                        location="spec.resource",
                    )

    materialized = functions._overlay(resource=materialized, overlay=forced_overlay)
    if err := check_for_celevalerror(
        materialized, location="spec.resource<security overlay>"
    ):
        return err

    # This can not be, it is for type checkers
    assert not isinstance(materialized, celpy.CELEvalError)

    return materialized


async def _create_api_resource(
    api: kr8s.Api,
    resource_api: type[APIObject],
    namespace: str | None,
    create: structure.Create,
    owned_resource: bool,
    owner: tuple[str, dict],
    inputs: dict[str, celtypes.Value],
    resource_view: celtypes.MapType | None,
    forced_overlay: celtypes.MapType,
    full_resource_name: str,
):
    if not resource_view:
        resource_view = celtypes.MapType()

    if create.overlay:
        match evaluate_overlay(
            overlay=create.overlay,
            inputs=inputs,
            base=resource_view,
            location="spec.create.overlay",
        ):
            case PermFail() as failure:
                return failure
            case celtypes.MapType() as resource_view:
                # Just needed to update resource_view
                pass
            case _ as bad_type:
                return PermFail(
                    message=f"Expected mapping, but received {type(bad_type)} for 'spec.create.overlay'",
                    location="spec.create.overlay",
                )

        if err := check_for_celevalerror(resource_view, location="spec.create.overlay"):
            return err

        # Purely for TypeChecker; check_for_celevalerror eliminates these.
        assert not isinstance(resource_view, celpy.CELEvalError)

    match functions._overlay(resource=resource_view, overlay=forced_overlay):
        case celpy.CELEvalError() as err:
            if updated_err := check_for_celevalerror(
                err, location="spec.create.overlay(name apply)"
            ):
                return updated_err

            # This should never happen.
            return PermFail(
                message=f"Error applying name/namesace overlay: {err}",
                location="spec.create.overlay(apply)",
            )
        case celtypes.MapType() as resource_view:
            pass

    if err := check_for_celevalerror(resource_view, location="spec.create.overlay"):
        return err

    owner_namespace, owner_ref = owner
    if owned_resource and owner_namespace == namespace:
        owner_refs = _updated_owner_refs(resource_view, owner_ref)
        if not is_unwrapped_ok(owner_refs):
            return owner_refs

        resource_view["metadata"]["ownerReferences"] = owner_refs

    converted_resource = convert_bools(resource_view)

    if not isinstance(converted_resource, (celtypes.MapType, dict)):
        return PermFail(
            message=f"Unexpected issue with encoding resource, received {type(converted_resource)}",
            location="<api-create-convert>",
        )

    new_resource = resource_api(
        api=api, resource=converted_resource, namespace=namespace
    )

    try:
        await new_resource.create()
    except Exception as err:
        # TODO: Probably need to catch timeouts and retry here.
        return PermFail(
            message=f"Error creating {full_resource_name}: {err}",
            location="spec.create",
        )

    return Retry(
        message=f"Creating {full_resource_name}.",
        delay=create.delay,
        location="spec.create",
    )


def _forced_overlay(resource_api: type[APIObject], name: str, namespace: str | None):
    # TODO: Make sure None doesn't break this, eg when creating Namespaces
    forced_overlay = celpy.json_to_cel(
        {
            "apiVersion": resource_api.version,
            "kind": resource_api.kind,
            "metadata": {"name": name, "namespace": namespace},
        }
    )

    # Perhaps this could occur with some corrupt name config?
    if not isinstance(forced_overlay, celtypes.MapType):
        return PermFail(
            message=f"Unexpected issue with kind/name-overlay, received {type(forced_overlay)}",
            location="<api-create-overlay>",
        )

    return forced_overlay


def _updated_owner_refs(resource_view: dict, owner_ref: dict):
    match resource_view:
        case {"metadata": metadata}:
            pass
        case _:
            return PermFail(
                message=f"Missing resource while adding owner ref",
                location="<api-add-owner>",
            )

    match metadata:
        case {"ownerReferences": owner_refs}:
            pass
        case dict():
            return [owner_ref]
        case _:
            return PermFail(
                message=f"Corrupt `metadata` while adding owner ref",
                location="<api-add-owner>",
            )

    if not owner_refs:
        return [owner_ref]

    if not isinstance(owner_refs, (list, tuple)):
        return PermFail(
            message=f"Corrupt `ownerReferences` while adding owner ref",
            location="<api-add-owner>",
        )

    trigger_uid = owner_ref.get("uid")

    for current_ref in owner_refs:
        if current_ref.get("uid") == trigger_uid:
            return owner_refs

    updated_owners = copy.deepcopy(owner_refs)
    updated_owners.append(owner_ref)
    return updated_owners


def _validate_owner_reffed(resource_view: dict, owner_ref: dict):
    match resource_view:
        case {"metadata": metadata}:
            pass
        case _:
            return PermFail(
                message=f"Missing resource while adding owner ref",
                location="<api-add-owner>",
            )

    match metadata:
        case {"ownerReferences": owner_refs}:
            pass
        case dict():
            return False
        case _:
            return PermFail(
                message=f"Corrupt `metadata` while adding owner ref",
                location="<api-add-owner>",
            )

    if not owner_refs:
        return False

    if not isinstance(owner_refs, (list, tuple)):
        return PermFail(
            message=f"Corrupt `ownerReferences` while adding owner ref",
            location="<api-add-owner>",
        )

    trigger_uid = owner_ref.get("uid")

    for current_ref in owner_refs:
        if current_ref.get("uid") == trigger_uid:
            return True

    return False


class ResourceMatch(NamedTuple):
    match: bool
    differences: Sequence[str]


def _validate_match(target, actual) -> ResourceMatch:
    """Compare the specified (`target`) state against the actual (`actual`)
    reosurce state. We compare all target fields and ignore anything extra.
    """

    match (target, actual):
        # Objects need a special comparator
        case dict(), dict():
            return _validate_dict_match(target, actual)
        case dict(), _:
            return ResourceMatch(
                match=False,
                differences=[f"<target:object, resource:{type(actual)}>"],
            )
        case _, dict():
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:object>"],
            )

        # Arrays need a special comparator
        case (list() | tuple(), list() | tuple()):
            return _validate_list_match(target, actual)
        case (list() | tuple(), _):
            return ResourceMatch(
                match=False,
                differences=[f"<target:array, resource:{type(actual)}>"],
            )
        case (_, list() | tuple()):
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:array>"],
            )

        # Bool needs a special comparator, due to Python's int truthiness rules
        case bool(), bool():
            if target == actual:
                return ResourceMatch(match=True, differences=())
            else:
                return ResourceMatch(
                    match=False,
                    differences=[f"<target:[{target}], resource:[{actual}]>"],
                )
        case bool(), _:
            return ResourceMatch(
                match=False,
                differences=[f"<target:bool, resource:{type(actual)}>"],
            )
        case _, bool():
            return ResourceMatch(
                match=False,
                differences=[f"<target:{type(target)}, resource:bool>"],
            )

    # Hopefully anything else is a simple type.
    if target == actual:
        return ResourceMatch(match=True, differences=())
    else:
        return ResourceMatch(
            match=False,
            differences=[f"<target:[{target}], resource:[{actual}]>"],
        )


def _validate_dict_match(target: dict, actual: dict) -> ResourceMatch:
    for target_key in target.keys():
        if target_key == "ownerReferences":
            continue

        if target_key not in actual:
            return ResourceMatch(
                match=False, differences=[f"<missing '{target_key}' in resource>"]
            )

        key_match = _validate_match(target[target_key], actual[target_key])
        if not key_match.match:
            differences = [f"'{target_key}'"]
            differences.extend(key_match.differences)
            return ResourceMatch(match=False, differences=differences)

    return ResourceMatch(match=True, differences=())


def _validate_list_match(target: list | tuple, actual: list | tuple) -> ResourceMatch:
    if len(target) != len(actual):
        return ResourceMatch(
            match=False,
            differences=[
                f"<length mismatch target:{len(target)}, actual:{len(actual)}"
            ],
        )

    for idx, (target_value, actual_value) in enumerate(zip(target, actual)):
        item_match = _validate_match(target_value, actual_value)
        if not item_match.match:
            differences = [f"at index '{idx}'"]
            differences.extend(item_match.differences)
            return ResourceMatch(match=False, differences=differences)

    return ResourceMatch(match=True, differences=())
