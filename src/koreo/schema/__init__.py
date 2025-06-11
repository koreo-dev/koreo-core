from importlib import resources
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("koreo.schema")

import fastjsonschema
import yaml

from koreo.constants import DEFAULT_API_VERSION
from koreo.function_test.structure import FunctionTest
from koreo.resource_function.structure import ResourceFunction
from koreo.resource_template.structure import ResourceTemplate
from koreo.result import PermFail
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow


NAME_CRD_MAP = {
    "functiontests.koreo.dev": FunctionTest,
    "resourcefunctions.koreo.dev": ResourceFunction,
    "resourcetemplates.koreo.dev": ResourceTemplate,
    "valuefunctions.koreo.dev": ValueFunction,
    "workflows.koreo.dev": Workflow,
}

_SCHEMA_VALIDATORS = {}


def validate(
    resource_type: type,
    spec: Any,
    schema_version: str | None = None,
    validation_required: bool = False,
):
    schema_validator = _get_validator(
        resource_type=resource_type, version=schema_version
    )
    if not schema_validator:
        if not validation_required:
            return None

        return PermFail(
            f"Schema validator not found for {resource_type.__name__} version {schema_version or DEFAULT_API_VERSION}",
        )

    try:
        schema_validator(spec)
    except fastjsonschema.JsonSchemaValueException as err:
        # This is hacky, and likely buggy, but it makes the messages easier to grok.
        validation_err = f"{err.rule_definition} {err}".replace(
            "data.", "spec."
        ).replace("data ", "spec ")
        return PermFail(validation_err)

    return None


def _get_validator(resource_type: type, version: str | None = None):
    if not _SCHEMA_VALIDATORS:
        load_bundled_schemas()

    if not version:
        version = DEFAULT_API_VERSION

    resource_version_key = f"{resource_type.__qualname__}:{version}"

    return _SCHEMA_VALIDATORS.get(resource_version_key)


def load_validator(resource_type_name: str, resource_schema: dict):
    spec = resource_schema.get("spec")
    if not spec:
        return None

    spec_names = spec.get("names")
    if spec_names:
        spec_kind = spec_names.get("kind", "<missing kind>")
    else:
        spec_kind = "<missing kind>"

    schema_specs = spec.get("versions")
    if not schema_specs:
        return None

    for schema_spec in schema_specs:
        version = schema_spec.get("name")
        if not version:
            continue

        schema_block = schema_spec.get("schema")
        if not schema_block:
            continue

        openapi_schema = schema_block.get("openAPIV3Schema")
        if not openapi_schema:
            continue

        openapi_properties = openapi_schema.get("properties")
        if not openapi_properties:
            continue

        openapi_spec = openapi_properties.get("spec")

        try:
            version_validator = fastjsonschema.compile(openapi_spec)
        except fastjsonschema.JsonSchemaDefinitionException:
            logger.exception(f"Failed to process {spec_kind} {version}")
            continue
        except AttributeError as err:
            logger.error(
                f"Probably encountered an empty `properties` block for {spec_kind} {version} (err: {err})"
            )
            raise

        resource_version_key = f"{resource_type_name}:{version}"
        _SCHEMA_VALIDATORS[resource_version_key] = version_validator


def load_validators_from_path(from_path: Path, clear_existing: bool = False):
    if clear_existing:
        _SCHEMA_VALIDATORS.clear()

    for resource in from_path.glob("*.yaml"):
        with resource.open() as crd_content:
            parsed = yaml.load_all(crd_content, Loader=yaml.SafeLoader)

            if not parsed:
                continue

            for chunk in parsed:
                chunk_kind = chunk.get("kind")
                if not chunk_kind or chunk_kind != "CustomResourceDefinition":
                    continue

                chunk_metadata = chunk.get("metadata")
                if not chunk_metadata:
                    continue

                chunk_resource = NAME_CRD_MAP.get(chunk_metadata.get("name"))
                if not chunk_resource:
                    continue

                load_validator(
                    resource_type_name=chunk_resource.__qualname__,
                    resource_schema=chunk,
                )


def load_bundled_schemas(clear_existing: bool = False):
    if clear_existing:
        _SCHEMA_VALIDATORS.clear()

    schema_module_resources = resources.path("koreo", "schema")

    with schema_module_resources as schema_path:
        load_validators_from_path(from_path=schema_path, clear_existing=clear_existing)
