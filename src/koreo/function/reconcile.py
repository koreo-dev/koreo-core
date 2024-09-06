import logging

import copy
import json

import kr8s
import jsonpath_ng

import celpy

from ..result import DepSkip, Ok, Outcome, PermFail, Retry, Skip, combine, is_ok

from .structure import Function


async def reconcile_function(
    api: kr8s.Api, function: Function, trigger_metadata: dict, trigger_spec: dict, inputs: dict
):
    converted_inputs = celpy.json_to_cel(inputs)

    if function.input_validators:
        validation_results = json.loads(
            json.dumps(function.input_validators.evaluate({"inputs": converted_inputs}))
        )
        validation_outcome = _predicate_to_koreo_result(validation_results)
        if not is_ok(validation_outcome):
            return validation_outcome

    managed_resource = copy.deepcopy(function.template)

    if function.materializers.base:
        base_overlay = json.loads(
            json.dumps(
                function.materializers.base.evaluate(
                    {
                        "inputs": converted_inputs,
                        "parent": celpy.json_to_cel({
                            "metadata": trigger_metadata,
                            "spec": trigger_spec
                        }),
                        "template": celpy.json_to_cel(managed_resource),
                    }
                )
            )
        )

        if base_overlay:
            for field_path, value in base_overlay.items():
                field_expr = jsonpath_ng.parse(field_path)
                field_expr.update_or_create(managed_resource, value)

    # TODO: Add owners, if ManagedResource.behaviors.add-owner

    resource = None
    if managed_resource:
        computed_metadata = managed_resource.get("metadata", {})

        name = computed_metadata.get("name")
        namespace = computed_metadata.get("namespace")

        if function.managed_resource.crd:
            api_version = function.managed_resource.crd.api_version
            kind = function.managed_resource.crd.kind
            plural = function.managed_resource.crd.plural
            namespaced = function.managed_resource.crd.namespaced
        else:
            api_version = managed_resource.get("apiVersion")
            kind = managed_resource.get("kind")
            plural = None
            namespaced = True if namespace else False

        if not (api_version and kind and name):
            logging.error("Missing critical managed resource fields!")
            raise Exception("FAILURE")

        resource_class = kr8s.objects.new_class(
            version=api_version,
            kind=kind,
            plural=plural,
            namespaced=namespaced,
            asyncio=True,
        )

        try:
            resource = await api.async_get(
                resource_class,
                name,
                namespace=namespace,
            )
        except kr8s.NotFoundError:
            resource = None
        except kr8s.ServerError as err:
            if err.response and err.response.status_code == 404:
                resource = None
            else:
                logging.exception(
                    f"ServerError loading {kind} resource {namespace}/{name}. ({type(err)}: {err})"
                )
                return Retry(
                    message=f"Non-404 ServerError loading {kind} resource {namespace}/{name}. ({type(err)}: {err})",
                    delay=30,
                )
        except Exception as err:
            logging.exception(
                f"Failure loading {kind} resource {namespace}/{name}. ({type(err)}: {err})"
            )
            return Retry(
                message=f"Error loading {kind} resource {namespace}/{name}. ({type(err)}: {err})",
                delay=30,
            )

        if not resource and function.managed_resource.behaviors.create:
            if function.materializers.on_create:
                on_create_overlay = json.loads(
                    json.dumps(
                        function.materializers.on_create.evaluate(
                            {
                                "inputs": converted_inputs,
                                "template": celpy.json_to_cel(managed_resource),
                            }
                        )
                    )
                )
                if on_create_overlay:
                    for field_path, value in on_create_overlay.items():
                        field_expr = jsonpath_ng.parse(field_path)
                        field_expr.update_or_create(managed_resource, value)

            logging.info(f"Creating {kind}.{api_version} resource {namespace}/{name}.")
            new_object = resource_class(
                api=api, resource=managed_resource, namespace=namespace
            )
            await new_object.create()
            # TODO: Dynamic create delay?
            return Retry(
                message=f"Creating {kind}.{api_version} resource {namespace}/{name}.",
                delay=30,
            )

        for resource_match in resource:
            if not _validate_match(managed_resource, resource_match.raw):
                logging.info(
                    f"{kind}.{api_version}/{name} resource did not match spec, updating!"
                )
                if function.managed_resource.behaviors.update == "patch":
                    # TODO: Should this only send fields with differences?
                    await resource_match.patch(managed_resource)
                    return Retry(
                        message=f"Updating {kind}.{api_version} resource {namespace}/{name} to match template.",
                        delay=5,
                    )
                elif function.managed_resource.behaviors.update == "recreate":
                    await resource_match.delete()
                    return Retry(
                        message=f"Deleting {kind}.{api_version} resource {namespace}/{name} to recreate.",
                        delay=5,
                    )
            else:
                logging.info(
                    f"{kind}.{api_version}/{name} resource matched spec, skipping!"
                )

    if function.outcome.tests:
        outcome_test_results = json.loads(
            json.dumps(
                function.outcome.tests.evaluate(
                    {
                        "inputs": converted_inputs,
                        "resource": (
                            celpy.json_to_cel(resource.raw) if resource else None
                        ),
                    }
                )
            )
        )
        outcome = _predicate_to_koreo_result(outcome_test_results)
        if not is_ok(outcome):
            return outcome

    if not function.outcome.ok_value:
        return Ok(None)

    return Ok(
        function.outcome.ok_value.evaluate(
            {
                "inputs": converted_inputs,
                "resource": celpy.json_to_cel(resource.raw) if resource else None,
            }
        )
    )


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
