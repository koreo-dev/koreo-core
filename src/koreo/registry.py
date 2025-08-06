from collections import defaultdict
from typing import NamedTuple, Sequence
import asyncio
import time
import logging

logger = logging.getLogger(name="koreo.registry")


class Resource[T](NamedTuple):
    resource_type: type[T]
    name: str
    namespace: str | None = None


class Kill: ...


class ResourceEvent[T](NamedTuple):
    resource: Resource[T]
    event_time: float


type RegistryQueue = asyncio.Queue[ResourceEvent | Kill]


def register[T](
    registerer: Resource[T],
    queue: RegistryQueue | None = None,
) -> RegistryQueue:
    if registerer in _SUBSCRIPTION_QUEUES:
        return _SUBSCRIPTION_QUEUES[registerer]

    if not queue:
        queue = asyncio.LifoQueue[ResourceEvent | Kill]()

    _SUBSCRIPTION_QUEUES[registerer] = queue

    event_time = time.monotonic()
    notify_subscribers(notifier=registerer, event_time=event_time)

    logger.debug(f"Registering {registerer}")

    return queue


class SubscriptionCycle(Exception): ...


def subscribe(subscriber: Resource, resource: Resource):
    _check_for_cycles(subscriber, (resource,))

    _RESOURCE_SUBSCRIBERS[resource].add(subscriber)
    _SUBSCRIBER_RESOURCES[subscriber].add(resource)

    logger.debug(f"{subscriber} subscribing to {resource}")


def subscribe_only_to(subscriber: Resource, resources: Sequence[Resource]):
    _check_for_cycles(subscriber, resources)

    current = _SUBSCRIBER_RESOURCES[subscriber]

    new = set(resources)

    for resource in new - current:
        _RESOURCE_SUBSCRIBERS[resource].add(subscriber)

    for resource in current - new:
        _RESOURCE_SUBSCRIBERS[resource].remove(subscriber)

    _SUBSCRIBER_RESOURCES[subscriber] = new

    logger.debug(f"{subscriber} subscribing to {resources}")


def unsubscribe(unsubscriber: Resource, resource: Resource):
    _RESOURCE_SUBSCRIBERS[resource].remove(unsubscriber)
    _SUBSCRIBER_RESOURCES[unsubscriber].remove(resource)


def notify_subscribers(notifier: Resource, event_time: float):
    subscribers = _RESOURCE_SUBSCRIBERS[notifier]
    if not subscribers:
        logger.debug(f"{notifier} has no subscribers")
        return

    active_subscribers = [
        _SUBSCRIPTION_QUEUES[subscriber]
        for subscriber in subscribers
        if subscriber in _SUBSCRIPTION_QUEUES
    ]

    if not active_subscribers:
        logger.debug(f"{notifier} has no active subscribers")
        return

    logger.debug(f"{notifier}:{event_time} notifying to {subscribers}")

    for subscriber in active_subscribers:
        try:
            subscriber.put_nowait(
                ResourceEvent(resource=notifier, event_time=event_time)
            )
        except asyncio.QueueFull:
            pass
            # TODO: I think there is a way to monitor for stalled subscribers
            # then notify a house-keeper process to deal with it.

            # health_check_task = asyncio.create_task()
            # _CHECK_SUBSCRIBER_HEALTH.add(health_check_task)
            # health_check_task.add_done_callback(_CHECK_SUBSCRIBER_HEALTH.discard)


def get_subscribers(resource: Resource):
    return _RESOURCE_SUBSCRIBERS[resource]


def get_subscriptions(resource: Resource):
    return _SUBSCRIBER_RESOURCES[resource]


def kill_resource(resource: Resource) -> RegistryQueue | None:
    if resource not in _SUBSCRIPTION_QUEUES:
        return None

    _kill_resource(resource)


def deregister(deregisterer: Resource, deregistered_at: float):
    # This resource is no longer following any resources.
    subscribe_only_to(subscriber=deregisterer, resources=[])

    # Remove this resource's subscription queue
    if deregisterer in _SUBSCRIPTION_QUEUES:
        queue = _kill_resource(resource=deregisterer)
        assert queue  # Just for the type-checker

        del _SUBSCRIPTION_QUEUES[deregisterer]

        # This is to prevent blocking anything waiting for this resource to do
        # something.
        while not queue.empty():
            try:
                queue.get_nowait()
                queue.task_done()
            except asyncio.QueueEmpty:
                break

    # Inform subscribers of a change
    notify_subscribers(notifier=deregisterer, event_time=deregistered_at)


def _kill_resource(resource: Resource) -> RegistryQueue | None:
    queue = _SUBSCRIPTION_QUEUES[resource]
    try:
        queue.put_nowait(Kill())

    except asyncio.QueueShutDown:
        return queue

    except asyncio.QueueFull:
        pass

    queue.shutdown()

    return queue


def _check_for_cycles(subscriber: Resource, resources: Sequence[Resource]):
    # Simple, inefficient cycle detection. This is a simple brute-force check,
    # which hopefully given the problem space is sufficient.
    to_check: set[Resource] = set(resources)
    while to_check:
        if subscriber in to_check:
            raise SubscriptionCycle(f"Detected subscription cycle due to {subscriber}")

        next_check_set = set[Resource]()
        for check_resource in to_check:
            if check_resource not in _SUBSCRIBER_RESOURCES:
                continue

            next_check_set.update(_SUBSCRIBER_RESOURCES[check_resource])
        to_check = next_check_set


_RESOURCE_SUBSCRIBERS: defaultdict[Resource, set[Resource]] = defaultdict(set[Resource])
_SUBSCRIBER_RESOURCES: defaultdict[Resource, set[Resource]] = defaultdict(set[Resource])
_SUBSCRIPTION_QUEUES: dict[Resource, RegistryQueue] = {}


def _reset_registries():
    _RESOURCE_SUBSCRIBERS.clear()
    _SUBSCRIBER_RESOURCES.clear()

    for queue in _SUBSCRIPTION_QUEUES.values():
        try:
            queue.put_nowait(Kill())
        except (asyncio.QueueFull, asyncio.QueueShutDown):
            pass

        try:
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
        except asyncio.QueueEmpty:
            pass

    _SUBSCRIPTION_QUEUES.clear()
