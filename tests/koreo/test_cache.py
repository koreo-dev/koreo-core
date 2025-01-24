from typing import Sequence
from collections import defaultdict
import asyncio
import random
import string
import unittest

from koreo.cache import (
    delete_from_cache,
    get_resource_from_cache,
    prepare_and_cache,
    _reset_cache,
    _REPREPARE_TASKS,
)

from koreo import result
from koreo import registry


def _name_generator():
    return "".join(random.choices(string.ascii_lowercase, k=15))


async def dangerous_prepare(key: str, value_spec: dict):
    raise Exception("Unexpected Prepare!")


async def failed_prepare(key: str, value_spec: dict):
    return result.PermFail("Failed to prepare.")


class ResourceTypeOne:
    prep_count: int
    name: str

    def __init__(self, **values):
        self.prep_count = values.get("prep_count", 0)
        self.name = values.get("name", "----MISSING----")

    def __str__(self):
        return f"<{self.__class__.__name__} prep_count={self.prep_count}>"


_PREP_COUNTER: dict[str, int] = defaultdict(int)


async def prepare_type_one(key: str, value_spec: dict):
    _PREP_COUNTER[key] += 1
    value_spec["prep_count"] = _PREP_COUNTER[key]

    return ResourceTypeOne(**value_spec), None


def build_preparer(resources: Sequence[registry.Resource]):
    async def preparer(key: str, value_spec: dict):
        _PREP_COUNTER[key] += 1
        value_spec["prep_count"] = _PREP_COUNTER[key]

        return ResourceTypeOne(**value_spec), resources

    return preparer


def build_timebomb_preparer(
    detonate_after: int, resources: Sequence[registry.Resource]
):
    async def preparer(key: str, value_spec: dict):
        # This is insanely dirty, never do this for real.
        _PREP_COUNTER[key] += 1
        value_spec["prep_count"] = _PREP_COUNTER[key]

        if _PREP_COUNTER[key] > detonate_after:
            raise Exception(f"Failed after '{detonate_after}' reprepares.")

        return ResourceTypeOne(**value_spec), resources

    return preparer


class TestCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_cache()
        _PREP_COUNTER.clear()

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
        resource_version = f"v{random.randint(10, 1000000)}"

        metadata = {"name": resource_name, "resourceVersion": resource_version}
        spec = {"name": resource_name}
        prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=prepare_type_one,
            metadata=metadata,
            spec=spec,
        )

        self.assertEqual(prepared.prep_count, _PREP_COUNTER[resource_name])
        self.assertEqual(prepared.name, resource_name)

        from_cache = get_resource_from_cache(
            resource_class=ResourceTypeOne, cache_key=resource_name
        )

        self.assertEqual(from_cache.prep_count, prepared.prep_count)
        self.assertEqual(from_cache.name, prepared.name)

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

        self.assertEqual(first_prepared.prep_count, _PREP_COUNTER[resource_name])
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

        self.assertEqual(second_prepared.prep_count, _PREP_COUNTER[resource_name])
        self.assertEqual(first_prepared.prep_count + 1, second_prepared.prep_count)
        self.assertEqual(first_prepared.name, second_prepared.name)

    async def test_prepare_fail(self):
        resource_name = _name_generator()
        resource_version = f"v{random.randint(10, 1000000)}"

        metadata = {"name": resource_name, "resourceVersion": resource_version}

        spec = {"name": resource_name}
        first_prepared = await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=failed_prepare,
            metadata=metadata,
            spec=spec,
        )

        self.assertIsInstance(first_prepared, result.PermFail)

    async def test_duplicate_not_reprepared(self):
        resource_name = _name_generator()
        resource_version = f"v{random.randint(10, 1000000)}"

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

        self.assertEqual(_PREP_COUNTER[resource_name], 1)
        self.assertEqual(first_prepared.prep_count, second_prepared.prep_count)
        self.assertEqual(first_prepared.name, second_prepared.name)


class TestCacheRegistry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_cache()
        _PREP_COUNTER.clear()

    async def test_repreparer_skip(self):
        # Resource names.
        resource_name = f"dependent-{_name_generator()}"
        watched_resource_name = f"watched-{_name_generator()}"

        # We want a Lifo queue for the test.
        check_queue = asyncio.LifoQueue()
        reg_resource = registry.Resource(
            resource_type=ResourceTypeOne, name=resource_name
        )
        registry.register(registerer=reg_resource, queue=check_queue)

        # Build a resource that watches.
        resource_version = f"v{random.randint(10, 1000000)}"
        metadata = {"name": resource_name, "resourceVersion": resource_version}
        spec = {"name": resource_name}
        await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=build_preparer(
                (
                    registry.Resource(
                        resource_type=ResourceTypeOne, name=watched_resource_name
                    ),
                )
            ),
            metadata=metadata,
            spec=spec,
        )

        # Build a resource that will be watched.
        watched_spec = {"name": watched_resource_name}

        for _ in range(5):
            watched_resource_version = f"v{random.randint(1000, 100000)}"
            watched_metadata = {
                "name": watched_resource_name,
                "resourceVersion": watched_resource_version,
            }
            await prepare_and_cache(
                resource_class=ResourceTypeOne,
                preparer=prepare_type_one,
                metadata=watched_metadata,
                spec=watched_spec,
            )

        await check_queue.join()
        registry.kill_resource(reg_resource)
        await check_queue.join()

        self.assertTrue(check_queue.empty())
        self.assertGreater(_PREP_COUNTER[resource_name], 1)
        self.assertLessEqual(_PREP_COUNTER[resource_name], 5)
        self.assertEqual(_PREP_COUNTER[watched_resource_name], 5)

    async def test_repreparer_failure(self):
        # Resource names.
        resource_name = f"dependent-{_name_generator()}"
        watched_resource_name = f"watched-{_name_generator()}"

        # We want a non-Lifo Queue for the test.
        check_queue = asyncio.Queue()
        reg_resource = registry.Resource(
            resource_type=ResourceTypeOne, name=resource_name
        )
        registry.register(registerer=reg_resource, queue=check_queue)

        # Build a resource that watches.
        resource_version = f"v{random.randint(10, 1000000)}"
        metadata = {"name": resource_name, "resourceVersion": resource_version}
        spec = {"name": resource_name}
        await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=build_timebomb_preparer(
                detonate_after=3,
                resources=(
                    registry.Resource(
                        resource_type=ResourceTypeOne, name=watched_resource_name
                    ),
                ),
            ),
            metadata=metadata,
            spec=spec,
        )

        # Build a resource that will be watched.
        watched_spec = {"name": watched_resource_name}

        for _ in range(5):
            watched_resource_version = f"v{random.randint(1000, 100000)}"
            watched_metadata = {
                "name": watched_resource_name,
                "resourceVersion": watched_resource_version,
            }
            await prepare_and_cache(
                resource_class=ResourceTypeOne,
                preparer=prepare_type_one,
                metadata=watched_metadata,
                spec=watched_spec,
            )

        await check_queue.join()

        self.assertTrue(check_queue.empty())
        self.assertGreater(_PREP_COUNTER[resource_name], 1)
        self.assertLessEqual(_PREP_COUNTER[resource_name], 5)
        self.assertEqual(_PREP_COUNTER[watched_resource_name], 5)

    async def test_delete_cleans_up(self):
        # Resource names.
        resource_name = f"dependent-{_name_generator()}"
        delete_resource_name = f"delete-{_name_generator()}"
        watched_resource_name = f"watched-{_name_generator()}"

        # Build a resource that watches.
        reg_resource = registry.Resource(
            resource_type=ResourceTypeOne, name=resource_name
        )
        reg_queue = registry.register(registerer=reg_resource)

        await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=build_preparer(
                resources=(
                    registry.Resource(
                        resource_type=ResourceTypeOne, name=delete_resource_name
                    ),
                ),
            ),
            metadata={
                "name": resource_name,
                "resourceVersion": f"v{random.randint(1000, 100000)}",
            },
            spec={"name": resource_name},
        )

        # Build a resource to delete that watches something else.
        del_resource = registry.Resource(
            resource_type=ResourceTypeOne, name=delete_resource_name
        )
        del_queue = registry.register(registerer=del_resource)
        await prepare_and_cache(
            resource_class=ResourceTypeOne,
            preparer=build_preparer(
                resources=(
                    registry.Resource(
                        resource_type=ResourceTypeOne, name=watched_resource_name
                    ),
                ),
            ),
            metadata={
                "name": delete_resource_name,
                "resourceVersion": f"v{random.randint(1000, 100000)}",
            },
            spec={"name": delete_resource_name},
        )

        # Build a resource that will be watched, and update it a few times.
        watched_spec = {"name": watched_resource_name}
        for _ in range(5):
            watched_resource_version = f"v{random.randint(1000, 100000)}"
            watched_metadata = {
                "name": watched_resource_name,
                "resourceVersion": watched_resource_version,
            }
            await prepare_and_cache(
                resource_class=ResourceTypeOne,
                preparer=prepare_type_one,
                metadata=watched_metadata,
                spec=watched_spec,
            )

        await del_queue.join()
        await reg_queue.join()

        # I'm not a huge fan of reaching in so deeply, but in this case we
        # want to make sure we cleared the task.

        self.assertEqual(len(_REPREPARE_TASKS), 2)

        to_be_deleted = get_resource_from_cache(
            resource_class=ResourceTypeOne, cache_key=delete_resource_name
        )
        self.assertIsNotNone(to_be_deleted)

        await delete_from_cache(
            resource_class=ResourceTypeOne, cache_key=delete_resource_name
        )

        await del_queue.join()
        await reg_queue.join()

        deleted = get_resource_from_cache(
            resource_class=ResourceTypeOne, cache_key=delete_resource_name
        )

        self.assertEqual(len(_REPREPARE_TASKS), 1)
        self.assertIsNone(deleted)
