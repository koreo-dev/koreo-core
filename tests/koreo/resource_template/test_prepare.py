import unittest

from celpy import celtypes

from koreo.result import PermFail, is_unwrapped_ok

from koreo.resource_template.prepare import prepare_resource_template


class TestPrepareResourceTemplate(unittest.IsolatedAsyncioTestCase):
    async def test_missing_spec(self):
        prepared = await prepare_resource_template("test-case", {})
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec`", prepared.message)

    async def test_missing_managed_resource(self):
        prepared = await prepare_resource_template("test-case", {"fake": True})
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("Missing `spec.managedResource`", prepared.message)

    async def test_missing_template(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
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

    async def test_good_config(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
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

        self.assertTrue(is_unwrapped_ok(prepared))
        prepared_template, _ = prepared

        self.assertIsInstance(
            prepared_template.template.get("spec", {}).get("bool"), celtypes.BoolType
        )
        self.assertEqual(prepared_template.managed_resource.api_version, "api.group/v1")
        self.assertEqual(prepared_template.managed_resource.kind, "TestResource")
