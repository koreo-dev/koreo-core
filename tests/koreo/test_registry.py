import asyncio
import time
import unittest

from koreo import registry


class ResourceA: ...


class ResourceB: ...


class TestRegistry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        registry._reset_registries()

    async def test_register_your_own_queue(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        the_queue = asyncio.LifoQueue()

        a_notifications = registry.register(resource_a, queue=the_queue)

        self.assertIs(a_notifications, the_queue)

    async def test_double_register(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")

        a_queue = registry.register(resource_a)

        for _ in range(5):
            self.assertIs(a_queue, registry.register(resource_a))

    async def test_basic_notifications(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")

        a_notifications = registry.register(resource_a)
        b_notifications = registry.register(resource_b)

        registry.subscribe(resource_a, resource_b)

        event_time = time.monotonic()
        registry.notify_subscribers(resource_b, event_time)

        notification, notification_time = await a_notifications.get()
        self.assertEqual(notification, resource_b)
        self.assertEqual(notification_time, event_time)
        a_notifications.task_done()

        self.assertTrue(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

    async def test_unsubscribe(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")

        a_notifications = registry.register(resource_a)
        b_notifications = registry.register(resource_b)

        registry.subscribe(resource_a, resource_b)
        registry.unsubscribe(resource_a, resource_b)

        registry.notify_subscribers(resource_b, time.monotonic())

        self.assertTrue(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

    async def test_deregister_subscriber(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")

        a_notifications = registry.register(resource_a)
        b_notifications = registry.register(resource_b)

        registry.subscribe(resource_a, resource_b)
        for _ in range(10):
            registry.notify_subscribers(resource_b, time.monotonic())

        # Ensure there are pending notifications for A (non for B).
        self.assertFalse(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

        registry.deregister(resource_a, time.monotonic())

        for _ in range(10):
            registry.notify_subscribers(resource_b, time.monotonic())

        # Ensure pending notifications for A are released.
        self.assertTrue(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

        # Ensure re-registering returns a new Queue
        a_notifications_new = registry.register(resource_a)
        self.assertIsNot(a_notifications, a_notifications_new)

    async def test_deregister_resource(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")

        a_notifications = registry.register(resource_a)
        b_notifications = registry.register(resource_b)

        registry.subscribe(resource_a, resource_b)

        self.assertTrue(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

        registry.deregister(resource_b, time.monotonic())

        notification, _ = await a_notifications.get()
        self.assertEqual(notification, resource_b)
        a_notifications.task_done()

        registry.notify_subscribers(resource_b, time.monotonic())

        # Notifications for the resource should still be sent.
        self.assertFalse(a_notifications.empty())
        self.assertTrue(b_notifications.empty())

    async def test_deregister_unregistered(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")

        b_notifications = registry.register(resource_b)

        registry.subscribe(resource_a, resource_b)
        for _ in range(10):
            registry.notify_subscribers(resource_b, time.monotonic())

        # Ensure there are pending notifications for A (non for B).
        self.assertTrue(b_notifications.empty())

        registry.deregister(resource_a, time.monotonic())

        for _ in range(10):
            registry.notify_subscribers(resource_b, time.monotonic())

        # Ensure pending notifications for A are released.
        self.assertTrue(b_notifications.empty())

    async def test_changing_subscriptions(self):
        resource_a = registry.Resource(resource_type=ResourceA, name="resource-1")
        resource_b = registry.Resource(resource_type=ResourceB, name="resource-2")
        resource_c = registry.Resource(
            resource_type=ResourceB, name="resource-3", namespace="old"
        )
        resource_d = registry.Resource(
            resource_type=ResourceB, name="resource-4", namespace="new"
        )
        resource_e = registry.Resource(
            resource_type=ResourceB, name="resource-5", namespace="new"
        )

        a_notifications = registry.register(resource_a)
        registry.register(resource_b)
        registry.register(resource_c)
        registry.register(resource_d)
        registry.register(resource_e)

        all_resources = set(
            [resource_a, resource_b, resource_c, resource_d, resource_e]
        )
        first_set = set([resource_b, resource_c])
        second_set = set([resource_b, resource_d, resource_e])

        for resource in first_set:
            registry.subscribe(resource_a, resource)

        for resource in all_resources:
            registry.notify_subscribers(resource, time.monotonic())

        while not a_notifications.empty():
            notifier, _ = await a_notifications.get()
            self.assertIn(notifier, first_set)
            a_notifications.task_done()

        self.assertTrue(a_notifications.empty())

        registry.subscribe_only_to(resource_a, second_set)

        for resource in all_resources:
            registry.notify_subscribers(resource, time.monotonic())

        while not a_notifications.empty():
            notifier, _ = await a_notifications.get()
            self.assertIn(notifier, second_set)
            a_notifications.task_done()

        self.assertTrue(a_notifications.empty())

    async def test_cycle_detection_self(self):
        resource = registry.Resource(resource_type=ResourceA, name="resource-1")
        with self.assertRaises(registry.SubscriptionCycle):
            registry.subscribe(resource, resource)

    async def test_cycle_detection(self):
        all_resources = [
            registry.Resource(resource_type=ResourceA, name=f"resource-{idx}")
            for idx in range(5)
        ]

        for idx, resource in enumerate(all_resources[1:]):
            registry.subscribe(all_resources[idx - 1], resource)

        with self.assertRaises(registry.SubscriptionCycle):
            registry.subscribe(all_resources[-1], all_resources[0])

    async def test_cycle_detection_sub_only_to(self):
        all_resources = [
            registry.Resource(resource_type=ResourceA, name=f"resource-{idx}")
            for idx in range(5)
        ]

        for idx, resource in enumerate(all_resources[:-1]):
            registry.subscribe_only_to(resource, all_resources[idx + 1 :])

        with self.assertRaises(registry.SubscriptionCycle):
            registry.subscribe(all_resources[-1], all_resources[0])
