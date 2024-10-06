import unittest

from celpy import celtypes

from koreo.result import PermFail

from koreo.resource_template.prepare import prepare_resource_template


class TestPrepareResourceTemplate(unittest.IsolatedAsyncioTestCase):
    async def test_missing_spec(self):
        prepared = await prepare_resource_template("test-case", {})
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec`", prepared.message)

    async def test_missing_template_name(self):
        prepared = await prepare_resource_template("test-case", {"managedResource": {}})
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec.templateName`", prepared.message)

    async def test_missing_managed_resource(self):
        prepared = await prepare_resource_template(
            "test-case", {"templateName": "hard-coded"}
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec.managedResource`", prepared.message)

    async def test_missing_template(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "hard-coded",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec.template`", prepared.message)

    async def test_bad_template_type(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "hard-coded",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "template": "bad value",
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("`spec.template` must be an object.", prepared.message)

    async def test_mismatched_apiVersion(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "hard-coded",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "template": {
                    "apiVersion": "different.api.group/v1",
                    "kind": "TestResource",
                },
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("`apiVersion` and `kind` must match", prepared.message)

    async def test_bad_context(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "hard-coded",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "template": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "context": "bad value",
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("must be an object.", prepared.message)

    async def test_template_name_type(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "=[1, 2, 3]",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "template": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "context": {"values": "ok"},
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("must evaluate to a string", prepared.message)

    async def test_good_config(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "templateName": "=managedResource.template_name('test')",
                "managedResource": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "template": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                    "spec": {"bool": True},
                },
                "context": {"values": "ok"},
            },
        )

        self.assertEqual(prepared.template_name, "TestResource.api.group/v1.test")
        self.assertIsInstance(
            prepared.template.get("spec", {}).get("bool"), celtypes.BoolType
        )
        self.assertEqual(prepared.managed_resource.api_version, "api.group/v1")
        self.assertEqual(prepared.managed_resource.kind, "TestResource")
