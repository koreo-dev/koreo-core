import logging

from koreo.result import Ok, PermFail

from . import structure
from .registry import index_resource_template


async def prepare_resource_template(
    cache_key: str, spec: dict | None
) -> structure.ResourceTemplate:
    logging.info(f"Prepare resource template {cache_key}")

    if not spec:
        spec = {}

    template_name = spec.get("templateName")

    managed_resource = _build_managed_resource(spec=spec.get("managedResource"))
    behavior = _load_behavior(spec=spec.get("behavior"))

    template = spec.get("template", {})

    # Validity Tests.
    valid = Ok(None)
    if not template_name:
        valid = PermFail(
            f'templateName ("{template_name}") is required for {cache_key}.'
        )

    if not (
        managed_resource
        and managed_resource.api_version == template.get("apiVersion")
        and managed_resource.kind == template.get("kind")
    ):
        valid = PermFail(
            f"Template resource spec must match managedResource configuration for {cache_key}."
        )

    template_key = f"{managed_resource.kind.lower()}.{managed_resource.api_version}.{template_name}"
    index_resource_template(cache_key=cache_key, template_key=template_key)

    return structure.ResourceTemplate(
        template_name=template_name,
        managed_resource=managed_resource,
        behavior=behavior,
        template=template,
        valid=valid,
    )


def _build_managed_resource(spec: dict | None) -> structure.ManagedResource | None:
    if not spec:
        return None

    kind = spec.get("kind")
    plural = spec.get("plural")

    return structure.ManagedResource(
        api_version=spec.get("apiVersion"),
        kind=kind,
        plural=plural if plural else f"{kind.lower()}s",
        namespaced=spec.get("namespaced", True),
    )


def _load_behavior(spec: dict | None) -> structure.Behavior:
    if not spec:
        spec = {}

    return structure.Behavior(
        load=spec.get("load", "name"),
        create=spec.get("create", True),
        update=spec.get("update", "patch"),
        delete=spec.get("delete", "destroy"),
    )
