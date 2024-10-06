import logging

import celpy
from celpy import celtypes

from koreo.result import PermFail, UnwrappedOutcome
from koreo.cel.encoder import encode_cel
from koreo.cel.functions import koreo_cel_functions, koreo_function_annotations

from . import structure
from .registry import index_resource_template


async def prepare_resource_template(
    cache_key: str, spec: dict | None
) -> UnwrappedOutcome[structure.ResourceTemplate]:
    logging.info(f"Prepare resource template {cache_key}")

    if not spec:
        return PermFail(f"Missing `spec` for ResourceTemplate '{cache_key}'.")

    template_name_exp_str = spec.get("templateName")

    if not template_name_exp_str:
        return PermFail(
            f"Missing `spec.templateName` for ResourceTemplate '{cache_key}'."
        )

    managed_resource_spec = spec.get("managedResource")
    managed_resource = _build_managed_resource(spec=managed_resource_spec)
    if not managed_resource:
        return PermFail(
            f"Missing `spec.managedResource` for ResourceTemplate '{cache_key}'."
        )

    template_spec = spec.get("template", {})
    template = celpy.json_to_cel(template_spec)
    if not template_spec:
        return PermFail(f"Missing `spec.template` for ResourceTemplate '{cache_key}'.")

    if not isinstance(template, celtypes.MapType):
        return PermFail(
            f"ResourceTemplate '{cache_key}' `spec.template` must be an object."
        )

    if not (
        managed_resource.api_version == template_spec.get("apiVersion")
        and managed_resource.kind == template_spec.get("kind")
    ):
        return PermFail(
            f"ResourceTemplate '{cache_key}' `apiVersion` and `kind` must match "
            f"in `spec.template` ('{template_spec.get("apiVersion")}', '{template_spec.get("kind")}') "
            f"and spec.managedResource ('{managed_resource.api_version}', '{managed_resource.kind}')."
        )

    behavior = _load_behavior(spec=spec.get("behavior", {}))

    context = celpy.json_to_cel(spec.get("context", {}))
    if not isinstance(context, celtypes.MapType):
        return PermFail(
            f"ResourceTemplate '{cache_key}' `spec.context` ('{context}') must be an object."
        )

    cel_env = celpy.Environment(annotations=koreo_function_annotations)

    template_name_expression = cel_env.program(
        cel_env.compile(encode_cel(template_name_exp_str)),
        functions=koreo_cel_functions,
    )
    template_key = celpy.CELJSONEncoder.to_python(
        template_name_expression.evaluate(
            {
                "managedResource": celpy.json_to_cel(managed_resource_spec),
                "template": template,
            }
        )
    )

    if not isinstance(template_key, celtypes.StringType):
        return PermFail(
            f"ResourceTemplate '{cache_key}' `spec.templateName` "
            f"('{template_name_exp_str}') must evaluate to a string ('{template_key}')."
        )

    index_resource_template(cache_key=cache_key, template_key=template_key)

    return structure.ResourceTemplate(
        template_name=template_key,
        managed_resource=managed_resource,
        behavior=behavior,
        context=context,
        template=template,
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
