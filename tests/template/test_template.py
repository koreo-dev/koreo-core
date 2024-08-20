from typing import Literal, TypedDict
import copy
import random
import string
import unittest

import celpy
import pykube

from koreo import Ok, Retry
from koreo.template import template


API_PATH = Literal["platform.konfig.realkinetic.com/v1"]


class DeeplyNested(TypedDict):
    name: str
    number: int


class Nested(TypedDict):
    name: str
    number: int
    deep: DeeplyNested


class ResourceTestSpec(TypedDict):
    name: str
    number: int
    literal_str: Literal["Konfig Identity Pool"]
    nested: Nested


class ResourceTest(TypedDict):
    apiVersion: API_PATH
    kind: Literal["TestRsource"]
    metadata: dict
    spec: ResourceTestSpec


class ResourceTestAPI(pykube.objects.NamespacedAPIObject):
    version: API_PATH = "platform.konfig.realkinetic.com/v1"
    endpoint = "testresources"
    kind = "TestRsource"


def _random_string():
    return "".join(random.choices(string.ascii_letters, k=random.randint(5, 50)))


class PrepareTests(unittest.TestCase):
    def setUp(self):
        template.__template_registry__ = {}

    def test_template_parse(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {},
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        self.assertFalse(prepared.cel_fields)
        self.assertEqual(test_resource_obj, prepared.template)

    def test_template_cache(self):
        uid = _random_string()
        generation = random.randint(1, 1000)

        cached_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": uid,
                "generation": generation,
                "annotations": {},
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        cached_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(cached_resource_obj)),
        )
        cached = template._prepare_template(template_resource=cached_resource)

        # This intentionally reuses the uid/generation to validate the original
        # object is returned. In a real environment, the UID or Generation change.
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": uid,
                "generation": generation,
                "annotations": {},
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        self.assertEqual(prepared, cached)

        # Just to document the intention
        self.assertEqual(prepared.template, cached_resource_obj)
        self.assertNotEqual(prepared.template, test_resource_obj)

    def test_template_cel_parse(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/cel-fields": '["spec.name"]'
                },
            },
            spec=ResourceTestSpec(
                name="1 + 1",
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        self.assertTrue(prepared.cel_fields)
        self.assertTrue(prepared.cel_fields["spec.name"].evaluate({}), "2")


class MaterializeTests(unittest.TestCase):
    def test_non_cel_template(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        self.assertEqual(materialized, test_resource_obj)

    def test_metadata_inherit_mode(self):
        inherited_metadata = {
            "name": _random_string(),
            "namespace": _random_string(),
            "uid": _random_string(),
            "generation": random.randint(1, 1000),
            "annotations": {},
            "labels": {"some.test/label": _random_string()},
        }

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "inherit",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        self.maxDiff = None
        materialized = template._materialize_template(
            parsed_template=prepared, metadata=inherited_metadata, inputs={}
        )

        self.assertEqual(materialized.get("spec"), test_resource_obj.get("spec"))
        self.assertEqual(materialized.get("metadata"), inherited_metadata)

    def test_missing_args(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/inputs": '["config", "metadata"]',
                    "templates.platform.konfig.realkinetic.com/cel-ok-check": "true",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        with self.assertRaisesRegex(ValueError, "arguments not provided"):
            template._materialize_template(
                parsed_template=prepared, metadata={}, inputs={}
            )

    def test_simple_cel_template(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-fields": '["spec.name"]',
                },
            },
            spec=ResourceTestSpec(
                name="string(1 + 1)",
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        self.maxDiff = None

        validate_resource = copy.deepcopy(test_resource_obj)
        validate_resource["spec"]["name"] = "2"

        self.assertEqual(materialized, validate_resource)

    def test_cel_template_with_inputs(self):
        name = _random_string()
        number = random.randint(50, 500)
        deep_name = _random_string()
        deep_number = random.randint(50, 500)

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-fields": '["spec.name", "spec.number", "spec.nested.deep.name", "spec.nested.deep.number"]',
                },
            },
            spec=ResourceTestSpec(
                name="inputs.name",
                number="inputs.number",
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name='"base-name/" + inputs.other.name',
                        number="inputs.result.number",
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared,
            metadata={},
            inputs=celpy.json_to_cel(
                {
                    "name": name,
                    "number": number,
                    "other": {"name": deep_name},
                    "result": {"number": deep_number},
                }
            ),
        )

        self.maxDiff = None

        validate_resource = copy.deepcopy(test_resource_obj)
        validate_resource["spec"].update({"name": name, "number": number})
        validate_resource["spec"]["nested"]["deep"] = {
            "name": f"base-name/{deep_name}",
            "number": deep_number,
        }

        self.assertEqual(materialized, validate_resource)

    def test_simple_cel_map_template(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-fields": '["spec.name"]',
                    "templates.platform.konfig.realkinetic.com/cel-field-map": '{"metadata.name": "name-" + string(1 + 2)}',
                },
            },
            spec=ResourceTestSpec(
                name="string(1 + 1)",
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared,
            metadata={"name": "WRONG", "namespace": "BAD-NAMESPACE"},
            inputs={},
        )

        self.maxDiff = None

        validate_resource = copy.deepcopy(test_resource_obj)
        validate_resource["metadata"]["name"] = "name-3"
        validate_resource["spec"]["name"] = "2"

        self.assertEqual(materialized, validate_resource)

    def test_cel_map_template_with_inputs(self):
        name = _random_string()
        number = random.randint(50, 500)
        deep_name = _random_string()
        deep_number = random.randint(50, 500)

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-field-map": '{"spec.name": inputs.name, "spec.number": inputs.number, "spec.nested.deep.name": "base-name/" + inputs.other.name, "spec.nested.deep.number": inputs.result.number}',
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared,
            metadata={},
            inputs=celpy.json_to_cel(
                {
                    "name": name,
                    "number": number,
                    "other": {"name": deep_name},
                    "result": {"number": deep_number},
                }
            ),
        )

        validate_resource = copy.deepcopy(test_resource_obj)
        validate_resource["spec"].update({"name": name, "number": number})
        validate_resource["spec"]["nested"]["deep"] = {
            "name": f"base-name/{deep_name}",
            "number": deep_number,
        }

        self.assertEqual(materialized, validate_resource)

    def test_cel_map_annotation(self):
        annotation = _random_string()

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-field-map": "{\"metadata.annotations['tests.platform.konfig.realkinetic.com/test-annotation']\": inputs.annotation}",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared,
            metadata={},
            inputs=celpy.json_to_cel({"annotation": annotation}),
        )

        self.assertEqual(
            materialized.get("metadata", {})
            .get("annotations", {})
            .get("tests.platform.konfig.realkinetic.com/test-annotation"),
            annotation,
        )

    def test_cel_map_metadata(self):
        name = _random_string()
        name_suffix = _random_string()

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": name,
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "inherit",
                    "templates.platform.konfig.realkinetic.com/cel-field-map": '{"metadata.name": inputs.metadata.name + "-" + inputs.name_suffix}',
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared,
            metadata={
                "apiVersion": "test.konfig.realkinetic.com/v1alpha1",
                "kind": "Test",
                "name": name,
                "namespace": "change-me",
            },
            inputs=celpy.json_to_cel({"name_suffix": name_suffix}),
        )

        self.assertEqual(
            materialized.get("metadata", {}).get("name", ""),
            f"{name}-{name_suffix}",
        )


class ReturnHelperTests(unittest.TestCase):
    def test_no_check_is_ok(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs={}
        )

        self.assertIsInstance(return_value, Ok)
        self.assertIs(return_value.data, None)

    def test_ok_check_is_ok(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-ok-check": "resource.spec.nested.deep.name == 'Result.Ok'",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name="Result.Ok", number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs={}
        )

        self.assertIsInstance(return_value, Ok)
        self.assertIs(return_value.data, None)

    def test_failed_check_is_retry(self):
        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-ok-check": "has(resource.status) ? (has(resource.status.ready) ? (resource.status.ready == 'True') : false) : false",
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name="Result.Ok", number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs={}
        )

        self.assertIsInstance(return_value, Retry)

    def test_simple_value_extract(self):
        name = _random_string()

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-ok-value": "resource.spec.name",
                },
            },
            spec=ResourceTestSpec(
                name=name,
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs={}
        )

        self.assertIsInstance(return_value, Ok)
        self.assertEqual(return_value.data, name)

    def test_complex_value_extract(self):
        name = _random_string()
        number = random.randint(0, 10000)

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-ok-value": '{"name": resource.spec.name, "number-sq": resource.spec.nested.number * resource.spec.nested.number}',
                },
            },
            spec=ResourceTestSpec(
                name=name,
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=number,
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs={}
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs={}
        )

        self.assertIsInstance(return_value, Ok)
        self.assertDictEqual(
            return_value.data, {"name": name, "number-sq": number * number}
        )

    def test_input_value_extract(self):
        name = _random_string()
        number = random.randint(0, 10000)

        input_name = _random_string()
        input_number = random.randint(0, 10000)

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": _random_string(),
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-ok-value": '{"name": inputs.name, "number": inputs.number}',
                },
            },
            spec=ResourceTestSpec(
                name=name,
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=number,
                    deep=DeeplyNested(
                        name=_random_string(), number=random.randint(0, 10000)
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        inputs = celpy.json_to_cel(
            {
                "name": input_name,
                "number": input_number,
            }
        )

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs=inputs
        )

        return_value = template.reconcile_result_helper(
            template=prepared, obj=materialized, inputs=inputs
        )

        self.assertIsInstance(return_value, Ok)
        self.assertDictEqual(
            return_value.data, {"name": input_name, "number": input_number}
        )


class OnCreateTests(unittest.TestCase):
    def test_new_field_setter(self):
        resource_name = _random_string()

        test_resource_obj = ResourceTest(
            apiVersion="platform.konfig.realkinetic.com/v1",
            kind="TestRsource",
            metadata={
                "name": resource_name,
                "namespace": _random_string(),
                "uid": _random_string(),
                "generation": random.randint(1, 1000),
                "annotations": {
                    "templates.platform.konfig.realkinetic.com/metadata-mode": "skip",
                    "templates.platform.konfig.realkinetic.com/cel-on-create-field-map": '{"spec.resourceID":  resource.metadata.name + (has(inputs.__params.create_suffix) ? "-" + inputs.__params.create_suffix : "")}',
                },
            },
            spec=ResourceTestSpec(
                name=_random_string(),
                number=random.randint(0, 10000),
                literal_str="Konfig Identity Pool",
                nested=Nested(
                    name=_random_string(),
                    number=random.randint(0, 10000),
                    deep=DeeplyNested(
                        name=_random_string(),
                        number=random.randint(0, 10000),
                    ),
                ),
            ),
        )

        test_resource = ResourceTestAPI(
            api=None,
            obj=dict(copy.deepcopy(test_resource_obj)),
        )

        prepared = template._prepare_template(template_resource=test_resource)

        inputs = celpy.json_to_cel({"__params": {"create_suffix": "suffix-testing"}})

        materialized = template._materialize_template(
            parsed_template=prepared, metadata={}, inputs=inputs
        )

        fully_prepped = template._materialize_template_v2(
            materialized_obj=materialized,
            cel_runner=prepared.cel_on_create_fields,
            inputs=inputs,
        )

        print(fully_prepped)

        self.assertEqual(
            fully_prepped.get("spec", {}).get("resourceID"),
            f"{resource_name}-suffix-testing",
        )
