from unittest.mock import AsyncMock
import asyncio
import unittest

import kr8s.asyncio

from koreo.resource_function.reconcile.kind_lookup import (
    get_plural_kind,
    _reset,
    _lookup_locks,
)


class TestGetFullKind(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        _reset()

    async def test_ok_lookup(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.return_value = (None, plural_kind, None)

        api_version = "unit.test/v1"

        result = await get_plural_kind(api_mock, "UnitTest", api_version)

        self.assertEqual(result, plural_kind)
        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_successive_lookups(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.return_value = (None, plural_kind, None)

        api_version = "unit.test/v1"

        result = await get_plural_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, plural_kind)

        result = await get_plural_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, plural_kind)

        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_multiple_requests(self):
        plural_kind = "unittesties"

        async def lookup(_):
            await asyncio.sleep(0)
            return (None, plural_kind, None)

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = lookup

        api_version = "unit.test/v1"

        tasks = [
            asyncio.create_task(get_plural_kind(api_mock, "UnitTest", api_version)),
            asyncio.create_task(get_plural_kind(api_mock, "UnitTest", api_version)),
            asyncio.create_task(get_plural_kind(api_mock, "UnitTest", api_version)),
        ]
        done, pending = await asyncio.wait(tasks)

        self.assertEqual(len(tasks), len(done))
        self.assertEqual(0, len(pending))

        for task in tasks:
            self.assertEqual(task.result(), plural_kind)

        self.assertEqual(1, api_mock.lookup_kind.call_count)

    async def test_missing_kind(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = ValueError("Kind not found")

        api_version = "unit.test/v1"

        plural_kind = await get_plural_kind(api_mock, "UnitTest", api_version)

        self.assertIsNone(plural_kind)

    async def test_2_timeout_retries(self):
        plural_kind = "unittesties"

        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = [
            asyncio.TimeoutError(),
            (None, plural_kind, None),
        ]

        api_version = "unit.test/v1"

        result = await get_plural_kind(api_mock, "UnitTest", api_version)
        self.assertEqual(result, plural_kind)

    async def test_too_many_timeout_retries(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = asyncio.TimeoutError()

        api_version = "unit.test/v1"

        with self.assertRaises(Exception):
            await get_plural_kind(api_mock, "unittest", api_version)

    async def test_lock_timeout(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = asyncio.TimeoutError()

        api_version = "unit.test/v1"

        lock_mock = AsyncMock(asyncio.Event)
        lock_mock.wait.side_effect = asyncio.TimeoutError()

        lookup_kind = f"unittest.{api_version}"
        _lookup_locks[lookup_kind] = lock_mock

        with self.assertRaises(Exception):
            await get_plural_kind(api_mock, "unittest", api_version)

    async def test_lock_cleared_on_errors(self):
        api_mock = AsyncMock(kr8s.asyncio.Api)
        api_mock.lookup_kind.side_effect = ZeroDivisionError("Unit Test")

        api_version = "unit.test/v1"

        with self.assertRaises(ZeroDivisionError):
            await get_plural_kind(api_mock, "unittest", api_version)
