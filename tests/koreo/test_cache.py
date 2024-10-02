import random
import string
import unittest

from koreo.cache import (
    get_resource_from_cache,
    prepare_and_cache,
    reprepare_and_update_cache,
    _reset_cache,
)


def _name_generator():
    return "".join(random.choices(string.ascii_lowercase, k=15))


async def dangerous_prepare(key: str, value_spec: dict):
    raise Exception("Unexpected Prepare!")


class ResourceTypeOne:
    prep_count: int
    name: str

    def __init__(self, **values):
        self.prep_count = values.get("prep_count", 0)
        self.name = values.get("name", "----MISSING----")

    def __str__(self):
        return f"<{self.__class__.__name__} prep_count={self.prep_count}>"


async def prepare_type_one(key: str, value_spec: dict):
    # This is insanely dirty, never do this for real.
    value_spec["prep_count"] = value_spec.get("prep_count", 0) + 1

    return ResourceTypeOne(**value_spec)


class TestCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_cache()

    async def test_get_missing(self):
        from_cache = get_resource_from_cache(
            resource_class=ResourceTypeOne, cache_key=_name_generator()
        )

        self.assertIsNone(from_cache)

    async def test_prepare_bad_metadata(self):
        metadata = {}

        with self.assertRaisesRegex(TypeError, "resource name and version"):
            await prepare_and_cache(
                resource_class=ResourceTypeOne,
                preparer=prepare_type_one,
                metadata=metadata,
                spec={},
            )

    async def test_roundtrip(self):
        resource_name = _name_generator()
        resource_version = f"v{random.randint(1000, 1000000)}"

        metadata = {"name": resource_name, "resourceVersion": resource_version}

        spec = {"name": resource_name}
        prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            metadata=metadata,
            spec=spec,
        )

        self.assertEqual(prepared.prep_count, 1)
        self.assertEqual(prepared.name, resource_name)

        from_cache = get_resource_from_cache(
            resource_class=ResourceTypeOne, cache_key=resource_name
        )

        self.assertEqual(prepared.prep_count, from_cache.prep_count)
        self.assertEqual(prepared.name, from_cache.name)

        reprepared = await reprepare_and_update_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            cache_key=resource_name,
        )

        self.assertEqual(prepared.prep_count + 1, reprepared.prep_count)
        self.assertEqual(prepared.name, reprepared.name)

    async def test_changes_reprepared(self):
        resource_name = _name_generator()
        first_resource_version = f"v{random.randint(1000, 100000)}"

        first_metadata = {
            "name": resource_name,
            "resourceVersion": first_resource_version,
        }

        spec = {"name": resource_name}
        first_prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            metadata=first_metadata,
            spec=spec,
        )

        self.assertEqual(first_prepared.prep_count, 1)
        self.assertEqual(first_prepared.name, resource_name)

        second_resource_version = f"v{random.randint(100, 999)}"

        second_metadata = {
            "name": resource_name,
            "resourceVersion": second_resource_version,
        }

        second_prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            metadata=second_metadata,
            spec=spec,
        )

        self.assertEqual(first_prepared.prep_count + 1, second_prepared.prep_count)
        self.assertEqual(first_prepared.name, second_prepared.name)

    async def test_duplicate_not_reprepared(self):
        resource_name = _name_generator()
        resource_version = f"v{random.randint(1000, 100000)}"

        metadata = {"name": resource_name, "resourceVersion": resource_version}

        spec = {"name": resource_name}
        first_prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            metadata=metadata,
            spec=spec,
        )

        self.assertEqual(first_prepared.prep_count, 1)
        self.assertEqual(first_prepared.name, resource_name)

        second_prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=dangerous_prepare,
            metadata=metadata,
            spec=spec,
        )

        self.assertEqual(first_prepared.prep_count, second_prepared.prep_count)
        self.assertEqual(first_prepared.name, second_prepared.name)

    async def test_reprepare_missing(self):
        missing = await reprepare_and_update_cache(
            resource_class=ResourceTypeOne,
            preparer=dangerous_prepare,
            cache_key=_name_generator(),
        )

        self.assertIsNone(missing)
