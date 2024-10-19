import random
import string
import unittest

from koreo.cache import prepare_and_cache
from koreo.resource_template import registry
from koreo.resource_template import structure


def _name_generator(prefix):
    return f"{prefix}-{"".join(random.choices(string.ascii_letters, k=15))}"


async def _fake_prepare(key, _):
    return f"RESOURCE: {key}", None


class TestResourceTemplateRegistry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        registry._reset_registry()

    async def test_roundtrip(self):
        cache_key = _name_generator("cache")
        template_key = _name_generator("template")

        await prepare_and_cache(
            resource_class=structure.ResourceTemplate,
            preparer=_fake_prepare,
            metadata={
                "name": cache_key,
                "resourceVersion": _name_generator("resoure-version"),
            },
            spec={},
        )

        registry.index_resource_template(cache_key=cache_key, template_key=template_key)
        resource = registry.get_resource_template(template_key=template_key)

        cached_resource, updaters = await _fake_prepare(cache_key, None)

        self.assertEqual(cached_resource, resource)
        self.assertIsNone(updaters)

    async def test_get_missing(self):
        template_key = _name_generator("template")

        resource = registry.get_resource_template(template_key=template_key)

        self.assertIsNone(resource)
