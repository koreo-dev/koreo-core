from typing import Any
import logging
import pathlib

logger = logging.getLogger("koreo.schema")

import fastjsonschema
import yaml

from koreo.function_test.structure import FunctionTest
from koreo.resource_function.structure import ResourceFunction
from koreo.resource_template.structure import ResourceTemplate
from koreo.result import PermFail
from koreo.value_function.structure import ValueFunction
from koreo.workflow.structure import Workflow

DEFAULT_API_VERSION = "v1beta1"

CRD_ROOT = pathlib.Path(__file__).parent.parent.parent.joinpath("crd")

CRD_MAP = {
    FunctionTest: CRD_ROOT.joinpath("function-test.yaml"),
    ResourceFunction: CRD_ROOT.joinpath("resource-function.yaml"),
    ResourceTemplate: CRD_ROOT.joinpath("resource-template.yaml"),
    ValueFunction: CRD_ROOT.joinpath("value-function.yaml"),
    Workflow: CRD_ROOT.joinpath("workflow.yaml"),
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
    if not version:
        version = DEFAULT_API_VERSION

    resource_version_key = f"{resource_type.__qualname__}:{version}"

    return _SCHEMA_VALIDATORS.get(resource_version_key)


def _load_validators():
    _SCHEMA_VALIDATORS.clear()

    for resource_type, schema_file in CRD_MAP.items():
        resource_validators = _load_validator(schema_file)
        if not resource_validators:
            continue

        for version, validator in resource_validators:
            resource_version_key = f"{resource_type.__qualname__}:{version}"
            _SCHEMA_VALIDATORS[resource_version_key] = validator


def _load_validator(schema_file: pathlib.Path):

    if not schema_file.exists():
        return None

    with schema_file.open() as crd_content:
        parsed = yaml.load(crd_content, Loader=yaml.Loader)
        spec = parsed.get("spec")
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

        validators = []

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
                validators.append((version, fastjsonschema.compile(openapi_spec)))
            except fastjsonschema.JsonSchemaDefinitionException:
                logger.exception(f"Failed to process {spec_kind} {version}")
                pass
            except AttributeError as err:
                logger.error(
                    f"Probably encountered an empty `properties` block for {spec_kind} {version} (err: {err})"
                )
                raise

        return validators


_load_validators()
