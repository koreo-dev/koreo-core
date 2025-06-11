import importlib
from typing import Any
import logging
import pathlib

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


KIND_CRD_MAP = {
    "FunctionTest": FunctionTest,
    "ResourceFunction": ResourceFunction,
    "ResourceTemplate": ResourceTemplate,
    "ValueFunction": ValueFunction,
    "Workflow": Workflow,
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
        load_validators_from_files()

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


def load_validators_from_files(clear_existing: bool = False, path: str = None):
    if clear_existing:
        _SCHEMA_VALIDATORS.clear()

    crd_path = pathlib.Path(path) if path else None

    if crd_path:
        # Use the pathlib.Path directly - no context manager needed
        _process_crd_directory(crd_path)
    else:
        import koreo
        
        # First try to find CRD files at project root (typical during development)
        koreo_path = pathlib.Path(koreo.__file__).parent
        project_root = koreo_path.parent.parent  # go up from src/koreo to project root
        crd_dir = project_root / "crd"
        
        if crd_dir.exists():
            _process_crd_directory(crd_dir)
        else:
            # Fallback to package resources if CRDs are bundled
            try:
                with importlib.resources.path(koreo, "crd") as resource_crd_dir:
                    _process_crd_directory(resource_crd_dir)
            except (FileNotFoundError, ModuleNotFoundError):
                logger.warning("No CRD files found - schema validation will be unavailable")


def _process_crd_directory(crd_dir):
    for resource_file in crd_dir.iterdir():
        if not resource_file.suffix == ".yaml":
            continue

        try:
            with resource_file.open() as resource_data:
                parsed = yaml.load(resource_data, Loader=yaml.SafeLoader)
                if not parsed:
                    logger.error(f"Failed to parse CRD file '{resource_file.name}'")
                    continue

                # CRD files have kind: CustomResourceDefinition
                # The actual resource kind is in spec.names.kind
                if parsed.get("kind") != "CustomResourceDefinition":
                    logger.warning(
                        f"Expected CustomResourceDefinition, got '{parsed.get('kind')}' in file '{resource_file.name}'"
                    )
                    continue

                spec = parsed.get("spec", {})
                names = spec.get("names", {})
                resource_kind = names.get("kind")

                if not resource_kind or resource_kind not in KIND_CRD_MAP:
                    logger.warning(
                        f"Unknown resource kind '{resource_kind}' in file '{resource_file.name}'"
                    )
                    continue

                resource_type = KIND_CRD_MAP[resource_kind]
                load_validator(
                    resource_type_name=resource_type.__qualname__,
                    resource_schema=parsed,
                )
        except Exception as e:
            logger.error(f"Failed to load CRD file '{resource_file.name}': {e}")
            continue
