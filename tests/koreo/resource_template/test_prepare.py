import unittest

from celpy import celtypes

from koreo.result import PermFail, is_unwrapped_ok

from koreo.resource_template.prepare import prepare_resource_template


class TestPrepareResourceTemplate(unittest.IsolatedAsyncioTestCase):
    async def test_missing_spec(self):
        prepared = await prepare_resource_template("test-case", {})
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("spec must contain", prepared.message)

    async def test_missing_template(self):
        prepared = await prepare_resource_template(
            "test-case",
            {"context": {}},
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("must contain", prepared.message)

    async def test_bad_template_type(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "template": "bad value",
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("spec.template must be object", prepared.message)

    async def test_missing_apiVersion(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "template": {
                    "kind": "TestResource",
                },
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("`apiVersion` and `kind` must be set", prepared.message)

    async def test_bad_context(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
                "template": {
                    "apiVersion": "api.group/v1",
                    "kind": "TestResource",
                },
                "context": "bad value",
            },
        )
        self.assertIsInstance(prepared, PermFail)
        self.assertIn("must be object", prepared.message)

    async def test_good_config(self):
        prepared = await prepare_resource_template(
            "test-case",
            {
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
