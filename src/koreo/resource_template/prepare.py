import logging

import celpy
from celpy import celtypes

from koreo.result import PermFail, UnwrappedOutcome

from . import structure


async def prepare_resource_template(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[tuple[structure.ResourceTemplate, None]]:
    logging.info(f"Prepare resource template {cache_key}")

    if not spec:
        return PermFail(
            message=f"Missing `spec` for ResourceTemplate '{cache_key}'.",
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    managed_resource_spec = spec.get("managedResource")
    managed_resource = _build_managed_resource(spec=managed_resource_spec)
    if not managed_resource:
        return PermFail(
            message=f"Missing `spec.managedResource` for ResourceTemplate '{cache_key}'.",
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

    if not (
        managed_resource.api_version == template_spec.get("apiVersion")
        and managed_resource.kind == template_spec.get("kind")
    ):
        return PermFail(
            message=(
                f"ResourceTemplate '{cache_key}' `apiVersion` and `kind` must match "
                f"in `spec.template` ('{template_spec.get("apiVersion")}', '{template_spec.get("kind")}') "
                f"and spec.managedResource ('{managed_resource.api_version}', '{managed_resource.kind}')."
            ),
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    behavior = _load_behavior(spec=spec.get("behavior", {}))

    context = celpy.json_to_cel(spec.get("context", {}))
    if not isinstance(context, celtypes.MapType):
        return PermFail(
            message=f"ResourceTemplate '{cache_key}' `spec.context` ('{context}') must be an object.",
            location=f"prepare:ResourceTemplate:{cache_key}",
        )

    return (
        structure.ResourceTemplate(
            managed_resource=managed_resource,
            behavior=behavior,
            context=context,
            template=template,
        ),
        None,
    )


def _build_managed_resource(spec: dict | None) -> structure.ManagedResource | None:
    if not spec:
        return None

    kind = spec.get("kind")
    plural = spec.get("plural", f"{kind.lower()}s")

    return structure.ManagedResource(
        api_version=spec.get("apiVersion"),
        kind=kind,
        plural=plural,
        namespaced=spec.get("namespaced", True),
    )


def _load_behavior(spec: dict) -> structure.Behavior:
    return structure.Behavior(
        load=spec.get("load", "name"),
        create=spec.get("create", True),
        update=spec.get("update", "patch"),
        delete=spec.get("delete", "destroy"),
    )
