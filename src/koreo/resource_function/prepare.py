from typing import Sequence
import logging
import re

logger = logging.getLogger("koreo.valuefunction.prepare")

from kr8s._objects import APIObject
import kr8s

import celpy

from koreo import cache
from koreo import registry
from koreo import schema
from koreo.cel.functions import koreo_function_annotations
from koreo.cel.prepare import (
    Overlay,
    prepare_expression,
    prepare_map_expression,
    prepare_overlay_expression,
)
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.predicate_helpers import predicate_extractor
from koreo.result import (
    PermFail,
    Retry,
    UnwrappedOutcome,
    is_unwrapped_ok,
    unwrapped_combine,
)
from koreo.value_function.structure import ValueFunction

from . import structure

# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("Environment").setLevel(logging.WARNING)
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)

DEFAULT_CREATE_DELAY = 30
DEFAULT_DELETE_DELAY = 15
DEFAULT_PATCH_DELAY = 30


async def prepare_resource_function(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.ResourceFunction, set[registry.Resource]]]:
    logger.debug(f"Prepare ResourceFunction:{cache_key}")

    if error := schema.validate(
        resource_type=structure.ResourceFunction, spec=spec, validation_required=True
    ):
        return PermFail(
            error.message,
            location=_location(cache_key, "spec"),
        )

    env = celpy.Environment(annotations=koreo_function_annotations)

    used_vars = set[str]()

    match predicate_extractor(cel_env=env, predicate_spec=spec.get("preconditions")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.preconditions")
            )
        case None:
            preconditions = None
        case celpy.Runner() as preconditions:
            used_vars.update(extract_argument_structure(preconditions.ast))

    match prepare_map_expression(
        cel_env=env, spec=spec.get("locals"), location="spec.locals"
    ):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=_location(cache_key, "spec.locals"),
            )
        case None:
            local_values = None
        case celpy.Runner() as local_values:
            used_vars.update(extract_argument_structure(local_values.ast))

    match _prepare_api_config(cel_env=env, spec=spec.get("apiConfig")):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=_location(cache_key, "spec.apiConfig"),
            )
        case (resource_api, resource_id, own_resource, readonly):
            used_vars.update(extract_argument_structure(resource_id.ast))

    match _prepare_resource_template(cel_env=env, spec=spec, readonly=readonly):
        case PermFail() as err:
            return err

        case structure.ResourceTemplateRef(name=template_name) as resource_template:
            if template_name:
                used_vars.update(extract_argument_structure(template_name.ast))

        case structure.InlineResourceTemplate(template=template) as resource_template:
            if template:
                used_vars.update(extract_argument_structure(template.ast))

    match _prepare_overlays(cel_env=env, spec=spec.get("overlays")):
        case None:
            overlays = []
            used_value_functions: set[registry.Resource] = set()

        case (overlays, overlay_input_keys, used_value_functions):
            used_vars.update(overlay_input_keys)

    match _prepare_create(cel_env=env, spec=spec.get("create")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.create")
            )
        case structure.Create(overlay=overlay) as create:
            if overlay:
                used_vars.update(extract_argument_structure(overlay.values.ast))

    match _prepare_update(spec=spec.get("update")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.update")
            )
        case (_) as update:
            # We just needed `update` set.
            pass

    match predicate_extractor(cel_env=env, predicate_spec=spec.get("postconditions")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.postconditions")
            )
        case None:
            postconditions = None
        case celpy.Runner() as postconditions:
            used_vars.update(extract_argument_structure(postconditions.ast))

    match prepare_map_expression(
        cel_env=env, spec=spec.get("return"), location="spec.return"
    ):
        case PermFail(message=message) as err:
            return err
        case None:
            return_value = None
        case celpy.Runner() as return_value:
            used_vars.update(extract_argument_structure(return_value.ast))

    return (
        structure.ResourceFunction(
            name=cache_key,
            preconditions=preconditions,
            local_values=local_values,
            crud_config=structure.CRUDConfig(
                resource_api=resource_api,
                resource_id=resource_id,
                own_resource=own_resource,
                readonly=readonly,
                resource_template=resource_template,
                overlays=overlays,
                create=create,
                update=update,
            ),
            postconditions=postconditions,
            return_value=return_value,
            dynamic_input_keys=used_vars,
        ),
        used_value_functions,
    )


def _location(cache_key: str, extra: str | None = None) -> str:
    base = f"prepare:ResourceFunction:{cache_key}"
    if not extra:
        return base

    return f"{base}:{extra}"


def _prepare_api_config(
    cel_env: celpy.Environment, spec: dict
) -> tuple[type[APIObject], celpy.celpy.Runner, bool, bool] | PermFail:
    api_version = spec.get("apiVersion")
    kind = spec.get("kind")
    name = spec.get("name")
    if not (api_version and kind and name):
        return PermFail(
            message="`apiVersion`, `kind`, and `name` are required in `spec.apiConfig`"
        )

    plural = spec.get("plural")
    if not plural:
        plural = f"{kind.lower()}s"

    namespaced = spec.get("namespaced", True)
    owned = spec.get("owned", True)
    readonly = spec.get("readonly", False)

    resource_id_cel = {"name": name}

    namespace = spec.get("namespace")
    if namespace:
        resource_id_cel["namespace"] = namespace
    elif namespaced:
        return PermFail(
            message="`namespace` is required when `spec.apiConfig.namespaced` is `true`"
        )

    match prepare_map_expression(
        cel_env=cel_env, spec=resource_id_cel, location="spec.apiConfig"
    ):
        case PermFail(message=message):
            return PermFail(message=message)
        case None:
            return PermFail(message="Error processing `spec.apiConfig.name`")
        case celpy.Runner() as resource_id:
            # Just needed to set name
            pass

    resource_api = kr8s.objects.new_class(
        version=api_version,
        kind=kind,
        plural=plural,
        namespaced=namespaced,
        asyncio=True,
    )
    return (resource_api, resource_id, owned, readonly)


def _prepare_resource_template(
    cel_env: celpy.Environment, spec: dict, readonly: bool
) -> structure.InlineResourceTemplate | structure.ResourceTemplateRef | PermFail:
    match spec:
        case {"resource": resource_template}:
            match prepare_map_expression(
                cel_env=cel_env, spec=resource_template, location="spec.resource"
            ):
                case PermFail() as err:
                    return err
                case None:
                    if readonly:
                        return structure.InlineResourceTemplate()

                    return PermFail(
                        message=f"Empty resource template ({resource_template})",
                        location="spec.resource",
                    )
                case celpy.Runner() as template_expression:
                    return structure.InlineResourceTemplate(
                        template=template_expression
                    )

        case {"resourceTemplateRef": resource_template_ref}:
            name_cel = resource_template_ref.get("name")
            match prepare_expression(
                cel_env=cel_env, spec=name_cel, location="spec.resourceTemplateRef.name"
            ):
                case None:
                    return PermFail(
                        message=f"`name` is required in `spec.resourceTemplateRef`",
                        location="spec.resourceTemplateRef.name",
                    )
                case PermFail() as err:
                    return err
                case celpy.Runner() as name_expression:
                    return structure.ResourceTemplateRef(name=name_expression)

        case _:
            return PermFail(
                message="One of `resource` or `resourceTemplateRef` is required.",
                location="spec",
            )


INPUTS_NAME_PATTERN = re.compile(r"inputs.(?P<name>[^.[]+)?\[?.*")


def _prepare_overlays(cel_env: celpy.Environment, spec: list[dict] | None) -> (
    None
    | tuple[
        UnwrappedOutcome[
            Sequence[structure.ValueFunctionOverlay | structure.InlineOverlay]
        ],
        set[str],
        set[registry.Resource[ValueFunction]],
    ]
):
    if not spec:
        return None

    overlays = []
    used_inputs: set[str] = set()
    used_value_functions: set[registry.Resource[ValueFunction]] = set()

    for idx, overlay_spec in enumerate(spec):
        skip_if_spec = overlay_spec.pop("skipIf", None)

        match prepare_expression(
            cel_env=cel_env,
            spec=skip_if_spec,
            location=f"spec.overlays[{idx}].skipIf",
        ):
            case PermFail() as err:
                overlays.append(err)
                continue
            case None:
                skip_if = None
            case celpy.Runner() as skip_if:
                used_inputs.update(extract_argument_structure(skip_if.ast))

        match overlay_spec:
            case {"overlay": inline_spec, **extras}:
                if extras:
                    extra_keys = ", ".join(f"{key}" for key in extras.keys())
                    overlays.append(
                        PermFail(
                            message=f"may not specify {extra_keys} with overlay",
                            location=f"spec.overlays[{idx}].overlay",
                        )
                    )
                    continue

                match prepare_overlay_expression(
                    cel_env=cel_env,
                    spec=inline_spec,
                    location=f"spec.overlays[{idx}].overlay",
                ):
                    case None:
                        overlays.append(
                            PermFail(
                                message=f"Empty inline overlay ({overlay_spec})",
                                location=f"spec.overlays[{idx}].overlay",
                            )
                        )
                        continue
                    case PermFail() as err:
                        overlays.append(err)
                        continue
                    case Overlay() as overlay:
                        overlays.append(
                            structure.InlineOverlay(overlay=overlay, skip_if=skip_if)
                        )
                        used_inputs.update(
                            extract_argument_structure(overlay.values.ast)
                        )

            case {"overlayRef": {"name": overlay_name}, **extras}:
                used_value_functions.add(
                    registry.Resource(resource_type=ValueFunction, name=overlay_name)
                )

                inputs_spec = extras.pop("inputs", None)
                if extras:
                    extra_keys = ", ".join(f"{key}" for key in extras.keys())
                    overlays.append(
                        PermFail(
                            message=f"may not specify {extra_keys} with overlayRef",
                            location=f"spec.overlays[{idx}].overlayRef",
                        )
                    )
                    continue

                match prepare_map_expression(
                    cel_env=cel_env,
                    spec=inputs_spec,
                    location=f"spec.overlays[{idx}].inputs",
                ):
                    case PermFail() as err:
                        overlays.append(err)
                        continue
                    case None:
                        inputs = None
                    case celpy.Runner() as inputs:
                        used_inputs.update(extract_argument_structure(inputs.ast))

                overlay_resource = cache.get_resource_from_cache(
                    resource_class=ValueFunction, cache_key=overlay_name
                )
                if not is_unwrapped_ok(overlay_resource) or overlay_resource is None:
                    overlays.append(
                        Retry(
                            message=f"ValueFunction:{overlay_name} not ready ({overlay_resource})",
                            location=f"spec.overlays[{idx}].overlayRef",
                        )
                    )
                    continue

                provided_inputs: set[str] = set(
                    inputs_spec.keys() if inputs_spec else ()
                )
                needed_inputs: set[str] = {
                    match.group("name")
                    for match in (
                        INPUTS_NAME_PATTERN.match(key)
                        for key in overlay_resource.dynamic_input_keys
                    )
                    if match
                }
                missing_inputs = needed_inputs - provided_inputs
                if missing_inputs:
                    missing_input_keys = ", ".join(
                        f'"{missing}"' for missing in missing_inputs
                    )
                    overlays.append(
                        PermFail(
                            message=f"{overlay_name} expected the following inputs {missing_input_keys}.",
                            location=f"spec.overlays[{idx}].inputs",
                        )
                    )
                    continue

                overlays.append(
                    structure.ValueFunctionOverlay(
                        overlay=overlay_resource, skip_if=skip_if, inputs=inputs
                    )
                )

            case _:
                overlays.append(
                    PermFail(
                        message=f"Invalid overlays spec: {overlay_spec}",
                        location=f"spec.overlays[{idx}]",
                    )
                )

    return (unwrapped_combine(overlays), used_inputs, used_value_functions)


def _prepare_create(
    cel_env: celpy.Environment, spec: dict | None
) -> structure.Create | PermFail:
    if not spec:
        return structure.Create(enabled=True, delay=DEFAULT_CREATE_DELAY)

    enabled = spec.get("enabled", True)
    if not enabled:
        return structure.Create(enabled=False)

    delay = spec.get("delay", DEFAULT_CREATE_DELAY)

    overlay_spec = spec.get("overlay")
    match prepare_overlay_expression(
        cel_env=cel_env,
        spec=overlay_spec,
        location="spec.create.overlay",
    ):
        case PermFail() as err:
            return err
        case None:
            overlay = None
        case Overlay() as overlay:
            # Just needed overlay
            pass

    return structure.Create(enabled=enabled, delay=delay, overlay=overlay)


def _prepare_update(spec: dict | None) -> structure.Update | PermFail:
    match spec:
        case None:
            return structure.UpdatePatch(delay=DEFAULT_PATCH_DELAY)
        case {"patch": {"delay": delay}}:
            return structure.UpdatePatch(delay=delay)
        case {"recreate": {"delay": delay}}:
            return structure.UpdateRecreate(delay=delay)
        case {"never": {}}:
            return structure.UpdateNever()
        case _:
            return PermFail(
                message="Malformed `spec.update`, expected a mapping with `patch`, `recreate`, or `never`"
            )
