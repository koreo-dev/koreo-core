import logging

logger = logging.getLogger("koreo.resourcetemplate.prepare")

import celpy
from celpy import celtypes

from koreo import schema
from koreo.result import PermFail, UnwrappedOutcome

from . import structure


async def prepare_resource_template(
    cache_key: str, spec: dict
) -> UnwrappedOutcome[tuple[structure.ResourceTemplate, None]]:
    logger.debug(f"Prepare resource template {cache_key}")

    if error := schema.validate(
        resource_type=structure.ResourceTemplate, spec=spec, validation_required=True
    ):
        return PermFail(
            error.message,
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    template_spec = spec.get("template", {})
    template = celpy.json_to_cel(template_spec)
    if not template_spec:
        return PermFail(
            message=f"Missing `spec.template` for ResourceTemplate '{cache_key}'.",
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    if not isinstance(template, celtypes.MapType):
        return PermFail(
            message=f"ResourceTemplate '{cache_key}' `spec.template` must be an object.",
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    if not (template_spec.get("apiVersion") and template_spec.get("kind")):
        return PermFail(
            message=(
                f"ResourceTemplate '{cache_key}' `apiVersion` and `kind` must "
                "be set within `spec.template` ("
                f"apiVersion: '{template_spec.get("apiVersion")}', "
                f"kind: '{template_spec.get("kind")}') "
            ),
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    context = celpy.json_to_cel(spec.get("context", {}))
    if not isinstance(context, celtypes.MapType):
        return PermFail(
            message=f"ResourceTemplate '{cache_key}' `spec.context` ('{context}') must be an object.",
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    return (
        structure.ResourceTemplate(
            context=context,
            template=template,
        ),
        None,
    )
