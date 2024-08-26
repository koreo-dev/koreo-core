from typing import NamedTuple
import copy
import json
import logging
import re

import celpy
import celpy.celtypes as celtypes
import jsonpath_ng
import kopf
import pykube

from koreo.flow import Ok, Outcome, Retry, UnwrapOk
from . import template_cel_functions


# Try to reduce the incredibly verbose logging from celpy
logging.getLogger("NameContainer").setLevel(logging.WARNING)
logging.getLogger("Evaluator").setLevel(logging.WARNING)
logging.getLogger("evaluation").setLevel(logging.WARNING)
logging.getLogger("celtypes").setLevel(logging.WARNING)


class KonfigTemplate(NamedTuple):
    api_version: str
    kind: str
    namespace: str
    name: str
    version: str


class _ParsedTemplate(NamedTuple):
    template: dict
    required_inputs: list[str]
    cel_fields: dict[str, celpy.Runner]
    cel_map_fields: celpy.Runner | None
    cel_on_create_fields: celpy.Runner | None
    ok_check: celpy.Runner | None
    ok_value: celpy.Runner | None


class Template(NamedTuple):
    api: type[pykube.objects.APIObject]
    obj: dict
    parsed: _ParsedTemplate


def load_konfig_template(
    api: pykube.HTTPClient, template: KonfigTemplate, metadata: dict, inputs: dict
):
    loaded_template = _load_template(api=api, template=template)

    return Template(
        api=loaded_template.api,
        obj=_materialize_template(
            parsed_template=loaded_template.template,
            metadata=metadata,
            inputs=celpy.json_to_cel(inputs),
        ),
        parsed=loaded_template.template,
    )


def default_template_reconciler(
    api: pykube.HTTPClient,
    template: KonfigTemplate,
    adopt=True,
):
    loaded_template: _LoadedTemplate | None = None
    try:
        loaded_template = _load_template(api=api, template=template)
    except pykube.exceptions.ObjectDoesNotExist:
        pass

    # TODO: Use `loaded_template.template.required_inputs` to set the signature
    # spec.

    async def action(metadata: UnwrapOk[dict], **kwargs):
        nonlocal loaded_template
        if loaded_template is None:
            try:
                loaded_template = _load_template(api=api, template=template)
            except pykube.exceptions.ObjectDoesNotExist:
                pass

            if loaded_template is None:
                return Retry(
                    message=f"Failed to load {template.kind} template {template.name}/{template.version} in namespace {template.namespace}.",
                    delay=60,
                )

        converted_inputs: celtypes.Value = celpy.json_to_cel(kwargs)

        obj = _materialize_template(
            parsed_template=loaded_template.template,
            metadata=metadata,
            inputs=converted_inputs,
        )

        if adopt:
            kopf.adopt(obj)

        resource = loaded_template.api(api=api, obj=obj)
        try:
            resource.reload()
        except (pykube.ObjectDoesNotExist, pykube.exceptions.HTTPError):
            obj = _materialize_template_v2(
                materialized_obj=obj,
                cel_runner=loaded_template.template.cel_on_create_fields,
                inputs=converted_inputs,
            )
            loaded_template.api(api=api, obj=obj).create()

            # Alternatively, allow user to provide a name / description?
            template_kind = obj.get("kind")
            template_name = obj["metadata"].get("name")
            return Retry(
                message=f"Creating {template_kind} resource {template_name}", delay=30
            )

        # TODO: Add logic to diff fields and only do this if there are changes
        # to specified fields.

        # TODO: if diff changes, update and return a retry.

        try:
            loaded_template.api(api=api, obj=obj).update()
            resource.reload()
        except (pykube.ObjectDoesNotExist, pykube.exceptions.HTTPError) as err:
            logging.exception(f"Failed to update {json.dumps(obj)}")

            template_kind = obj.get("kind")
            template_name = obj["metadata"].get("name")
            return Retry(
                message=f"Error updating {template_kind} resource {template_name}. {err}",
                delay=30,
            )

        except Exception as err:
            logging.exception(f"Failed to update {json.dumps(obj)}")

            template_kind = obj.get("kind")
            template_name = obj["metadata"].get("name")
            return Retry(
                message=f"Error updating {template_kind} resource {template_name}. {err}",
                delay=30,
            )

        return reconcile_result_helper(
            template=loaded_template.template, obj=resource.obj, inputs=converted_inputs
        )

    return action


_version_checker = re.compile(r"v\d+[a-z]*\d*")


def _valid_version(template: KonfigTemplate):
    if template.version == "latest" or _version_checker.match(template.version):
        return True

    return False


__template_registry__: dict[str, _ParsedTemplate] = {}


class _LoadedTemplate(NamedTuple):
    api: type[pykube.objects.APIObject]
    template: _ParsedTemplate


def _load_template(api: pykube.HTTPClient, template: KonfigTemplate):
    if not _valid_version(template=template):
        raise ValueError("Konfig Template version must match 'v\\d*' or 'latest'")

    template_api = pykube.object_factory(
        api=api, api_version=template.api_version, kind=template.kind
    )

    template_loader = template_api.objects(api=api, namespace=template.namespace)

    selector = {
        "templates.platform.konfig.realkinetic.com/name": template.name,
        "templates.platform.konfig.realkinetic.com/active": "true",
    }

    if template.version == "latest":
        selector["templates.platform.konfig.realkinetic.com/latest"] = "true"
    else:
        selector["templates.platform.konfig.realkinetic.com/version"] = template.version

    # This will blow up if multiple objects are returned; which is desired.
    template_resource = template_loader.get(selector=selector)

    if "status" in template_resource.obj:
        del template_resource.obj["status"]

    return _LoadedTemplate(
        api=template_api,
        template=_prepare_template(template_resource=template_resource),
    )


def _resource_cache_key(template_resource: pykube.objects.NamespacedAPIObject):
    template_meta = template_resource.metadata
    uid = template_meta.get("uid")
    generation = template_meta.get("generation")

    return f"{uid}/{generation}"


def _prepare_template(
    template_resource: pykube.objects.NamespacedAPIObject,
) -> _ParsedTemplate:
    cache_key = _resource_cache_key(template_resource=template_resource)

    registered_template = __template_registry__.get(cache_key)
    if registered_template:
        return registered_template

    cel_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/cel-fields"
    )

    cel_map_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/cel-field-map"
    )

    cel_on_create_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/cel-on-create-field-map"
    )

    cel_ok_check_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/cel-ok-check"
    )

    cel_ok_value_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/cel-ok-value"
    )

    template_obj = template_resource.obj

    if not (
        cel_annotation
        or cel_map_annotation
        or cel_on_create_annotation
        or cel_ok_check_annotation
        or cel_ok_value_annotation
    ):
        parsed_template = _ParsedTemplate(
            template=template_obj,
            required_inputs=[],
            cel_fields={},
            cel_map_fields=None,
            cel_on_create_fields=None,
            ok_check=None,
            ok_value=None,
        )
        __template_registry__[cache_key] = parsed_template
        return parsed_template

    # TODO: I believe we can extract and build this list from the compiled CEL.
    # Then we wouldn't require this as an annotation.
    inputs_annotation = template_resource.annotations.get(
        "templates.platform.konfig.realkinetic.com/inputs"
    )
    if inputs_annotation:
        required_inputs: list[str] = json.loads(inputs_annotation)
    else:
        required_inputs: list[str] = []

    cel_fields: dict[str, celpy.Runner] = {}

    # TODO: I'm not sure if there should be one or one-per-CEL Field of these?
    cel_env = celpy.Environment(annotations=template_cel_functions.function_annotations)
    cel_env.logger.setLevel(logging.WARNING)

    if cel_annotation:
        cel_field_list: list[str] = json.loads(cel_annotation)

        for field in cel_field_list:
            field_lookup = jsonpath_ng.parse(field)
            for cel_field in field_lookup.find(template_obj):
                cel_fields[f"{cel_field.full_path}"] = cel_env.program(
                    cel_env.compile(cel_field.value),
                    functions=template_cel_functions.functions,
                )

    cel_map_fields = None
    if cel_map_annotation:
        cel_map_fields = cel_env.program(
            cel_env.compile(cel_map_annotation),
            functions=template_cel_functions.functions,
        )

    on_create_fields = None
    if cel_on_create_annotation:
        on_create_fields = cel_env.program(
            cel_env.compile(cel_on_create_annotation),
            functions=template_cel_functions.functions,
        )

    ok_check = None
    if cel_ok_check_annotation:
        ok_check = cel_env.program(
            cel_env.compile(cel_ok_check_annotation),
            functions=template_cel_functions.functions,
        )

    ok_value = None
    if cel_ok_value_annotation:
        ok_value = cel_env.program(
            cel_env.compile(cel_ok_value_annotation),
            functions=template_cel_functions.functions,
        )

    parsed_template = _ParsedTemplate(
        template=template_obj,
        required_inputs=required_inputs,
        cel_fields=cel_fields,
        cel_map_fields=cel_map_fields,
        cel_on_create_fields=on_create_fields,
        ok_check=ok_check,
        ok_value=ok_value,
    )

    __template_registry__[cache_key] = parsed_template

    return parsed_template


def _materialize_template(
    parsed_template: _ParsedTemplate, metadata: dict, inputs: celtypes.Value
):
    template = copy.deepcopy(parsed_template.template)

    metadata_mode = (
        template.get("metadata", {})
        .get("annotations", {})
        .get("templates.platform.konfig.realkinetic.com/metadata-mode", "inherit")
    )
    if metadata_mode == "inherit":
        template["metadata"] = copy.deepcopy(metadata)
        inputs["metadata"] = celpy.json_to_cel(metadata)

    missing_args = set(parsed_template.required_inputs).difference(inputs.keys())
    if missing_args:
        logging.info(f"required_inputs {parsed_template.required_inputs}")
        logging.info(f"input keys {inputs.keys()}")
        raise ValueError(
            f"Required template arguments not provided, missing {list(missing_args)}."
        )

    if not (parsed_template.cel_fields or parsed_template.cel_map_fields is not None):
        return template

    for field_path, cel_expression in parsed_template.cel_fields.items():
        cel_expression.logger.setLevel(logging.WARNING)
        try:
            # TODO: There's got a be something better than dumps / loads?
            value = json.loads(json.dumps(cel_expression.evaluate({"inputs": inputs})))
        except (celpy.CELEvalError, celpy.CELParseError) as e:
            raise Exception(f"CEL Error ({e}) evaluating {cel_expression}")
        except:
            logging.exception("Failed evaluating CEL")
            raise

        field_lookup = jsonpath_ng.parse(field_path)
        field_lookup.update_or_create(template, value)

    template = _materialize_template_v2(
        materialized_obj=template,
        cel_runner=parsed_template.cel_map_fields,
        inputs=inputs,
    )

    return template


def _materialize_template_v2(
    materialized_obj: dict,
    cel_runner: celpy.Runner | None,
    inputs: celtypes.Value,
):
    if cel_runner is None:
        return materialized_obj

    cel_runner.logger.setLevel(logging.WARNING)
    try:
        values = cel_runner.evaluate(
            {"resource": celpy.json_to_cel(materialized_obj), "inputs": inputs}
        )
    except (celpy.CELEvalError, celpy.CELParseError) as e:
        raise Exception(f"CEL Error ({e}) evaluating {cel_runner}")
    except:
        logging.exception(f"Failed evaluating CEL {cel_runner}")
        raise

    try:
        parsed_values = json.loads(json.dumps(values))
    except (celpy.CELEvalError, celpy.CELParseError) as e:
        raise Exception(f"JSON CEL Load Error ({e}) evaluating {values}")
    except:
        logging.exception(f"Failed evaluating JSON CEL {values}")
        raise

    for field_path, value in parsed_values.items():
        field_expr = jsonpath_ng.parse(field_path)
        field_expr.update_or_create(materialized_obj, value)

    return materialized_obj


def reconcile_result_helper(
    template: _ParsedTemplate, obj: dict, inputs: celtypes.Value
) -> Outcome:
    if template.ok_check:
        template.ok_check.logger.setLevel(logging.WARNING)
        is_ok = template.ok_check.evaluate({"resource": celpy.json_to_cel(obj)})

        if not is_ok:
            template_kind = obj.get("kind")
            template_name = obj["metadata"].get("name")
            return Retry(
                message=f"{template_kind} resource {template_name} is not ok.",
                delay=60,
            )

    if template.ok_value:
        template.ok_value.logger.setLevel(logging.WARNING)

        value = template.ok_value.evaluate(
            {"resource": celpy.json_to_cel(obj), "inputs": inputs}
        )

        # TODO: There's got a be something better than dumps / loads?
        try:
            return Ok(json.loads(json.dumps(value)))
        except (celpy.CELEvalError, celpy.CELParseError) as e:
            return Retry(
                message=f"CEL Error ({e}) evaluating OK Value returned {value}",
                delay=90,
            )
        except Exception as e:
            return Retry(
                message=f"Unknown Error ({e}) evaluating OK Value returned {value}",
                delay=90,
            )

    return Ok(None)
