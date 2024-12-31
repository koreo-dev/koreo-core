import logging

logger = logging.getLogger("koreo.valuefunction.prepare")

from kr8s._objects import APIObject
import kr8s

import celpy

from koreo import schema
from koreo.cel.encoder import encode_cel, encode_cel_template
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations
from koreo.cel.prepare import prepare_map_expression
from koreo.cel.structure_extractor import extract_argument_structure
from koreo.predicate_helpers import predicate_extractor
from koreo.result import PermFail, UnwrappedOutcome

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
) -> UnwrappedOutcome[tuple[structure.ResourceFunction, None]]:
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

    match predicate_extractor(cel_env=env, predicate_spec=spec.get("inputValidators")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.inputValidators")
            )
        case None:
            input_validators = None
        case celpy.Runner() as input_validators:
            used_vars.update(extract_argument_structure(input_validators.ast))

    match prepare_map_expression(
        cel_env=env, spec=spec.get("locals"), name="spec.locals"
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

        case structure.ResourceTemplateRef(
            name=template_name, overlay=template_overlay
        ) as resource_template:
            used_vars.update(extract_argument_structure(template_name.ast))
            if template_overlay:
                used_vars.update(extract_argument_structure(template_overlay.ast))

        case structure.InlineResourceTemplate(template=template) as resource_template:
            if template:
                used_vars.update(extract_argument_structure(template.ast))

    match _prepare_create(cel_env=env, spec=spec.get("create")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.create")
            )
        case structure.Create(overlay=overlay) as create:
            if overlay:
                used_vars.update(extract_argument_structure(overlay.ast))

    match _prepare_update(spec=spec.get("update")):
        case PermFail(message=message):
            return PermFail(
                message=message, location=_location(cache_key, "spec.update")
            )
        case (_) as update:
            # We just needed `update` set.
            pass

    match _prepare_outcome(cel_env=env, outcome_spec=spec.get("outcome")):
        case PermFail(message=message):
            return PermFail(
                message=message,
                location=f"prepare:ResourceFunction:{cache_key}.outcome",
            )
        case structure.Outcome(
            validators=validators, return_value=return_value
        ) as outcome:
            if validators:
                used_vars.update(extract_argument_structure(validators.ast))

            if return_value:
                used_vars.update(extract_argument_structure(return_value.ast))

    return (
        structure.ResourceFunction(
            input_validators=input_validators,
            local_values=local_values,
            crud_config=structure.CRUDConfig(
                resource_api=resource_api,
                resource_id=resource_id,
                own_resource=own_resource,
                readonly=readonly,
                resource_template=resource_template,
                create=create,
                update=update,
            ),
            outcome=outcome,
            dynamic_input_keys=used_vars,
        ),
        None,
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
        cel_env=cel_env, spec=resource_id_cel, name="spec.apiConfig"
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
                cel_env=cel_env, spec=resource_template, name="spec.resource"
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
            if not name_cel:
                return PermFail(
                    message=f"`name` is required in `spec.resourceTemplateRef`",
                    location="spec.resourceTemplateRef.name",
                )
            try:
                name_expression = cel_env.program(
                    cel_env.compile(encode_cel(name_cel)), functions=koreo_cel_functions
                )
            except celpy.CELParseError as err:
                return PermFail(
                    message=f"Parsing error ({err}) in ({name_cel})",
                    location="spec.resourceTemplateRef.name",
                )

            overlay_cel = resource_template_ref.get("overlay")
            if not overlay_cel:
                return structure.ResourceTemplateRef(name=name_expression, overlay=None)

            match prepare_map_expression(
                cel_env=cel_env,
                spec=overlay_cel,
                name="spec.resourceTemplateRef.overlay",
            ):
                case PermFail() as err:
                    return err
                case None:
                    return PermFail(
                        message=f"Empty overlay ({overlay_cel})",
                        location="spec.resourceTemplateRef.overlay",
                    )
                case celpy.Runner() as overlay_expression:
                    return structure.ResourceTemplateRef(
                        name=name_expression, overlay=overlay_expression
                    )

        case _:
            return PermFail(
                message="Either `resource` or `resourceTemplateRef` is required.",
                location="spec",
            )


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
    if not overlay_spec:
        overlay = None
    else:
        try:
            overlay = cel_env.program(
                cel_env.compile(encode_cel_template(overlay_spec)),
                functions=koreo_cel_functions,
            )
        except celpy.CELParseError as err:
            return PermFail(
                message=f"Parsing error ({err}) in ({overlay_spec})",
                location="spec.create.overlay",
            )

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


def _prepare_outcome(
    cel_env: celpy.Environment, outcome_spec: dict | None
) -> structure.Outcome | PermFail:
    if not outcome_spec:
        return structure.Outcome(validators=None, return_value=None)

    match predicate_extractor(
        cel_env=cel_env, predicate_spec=outcome_spec.get("validators")
    ):
        case PermFail(message=message):
            return PermFail(message=message)
        case None:
            validators = None
        case celpy.Runner() as validators:
            # Just needed to set validators
            pass

    match prepare_map_expression(
        cel_env=cel_env, spec=outcome_spec.get("return"), name="spec.outcome.return"
    ):
        case PermFail(message=message):
            return PermFail(message=message)
        case None:
            return_value = None
        case celpy.Runner() as return_value:
            # Just needed to set return_value
            pass

    return structure.Outcome(validators=validators, return_value=return_value)
