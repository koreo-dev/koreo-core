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


def register[
    T
](registerer: Resource[T], queue: RegistryQueue | None = None,) -> RegistryQueue:
    registerer_key = _resource_key(registerer)

    if registerer_key in _SUBSCRIPTION_QUEUES:
        return _SUBSCRIPTION_QUEUES[registerer_key]

    if not queue:
        queue = asyncio.LifoQueue[ResourceEvent | Kill]()

    _SUBSCRIPTION_QUEUES[registerer_key] = queue

    event_time = time.monotonic()
    notify_subscribers(notifier=registerer, event_time=event_time)

    logger.info(f"Registering {registerer}")

    return queue


class SubscriptionCycle(Exception): ...


def subscribe(subscriber: Resource, resource: Resource):
    subscriber_key = _resource_key(subscriber)
    resource_key = _resource_key(resource)

    _check_for_cycles(subscriber_key, (resource_key,))

    _RESOURCE_SUBSCRIBERS[resource_key].add(subscriber_key)
    _SUBSCRIBER_RESOURCES[subscriber_key].add(resource_key)

    logger.info(f"{subscriber} subscribing to {resource}")


def subscribe_only_to(subscriber: Resource, resources: Sequence[Resource]):
    subscriber_key = _resource_key(subscriber)

    new = set(_resource_key(subscribe_to) for subscribe_to in resources)
    _check_for_cycles(subscriber_key, list(new))

    current = _SUBSCRIBER_RESOURCES[subscriber_key]

    for resource_key in new - current:
        _RESOURCE_SUBSCRIBERS[resource_key].add(subscriber_key)

    for resource_key in current - new:
        _RESOURCE_SUBSCRIBERS[resource_key].remove(subscriber_key)

    _SUBSCRIBER_RESOURCES[subscriber_key] = new

    logger.info(f"{subscriber} subscribing to {resources}")


def unsubscribe(unsubscriber: Resource, resource: Resource):
    unsubscriber_key = _resource_key(unsubscriber)
    resource_key = _resource_key(resource)

    _RESOURCE_SUBSCRIBERS[resource_key].remove(unsubscriber_key)
    _SUBSCRIBER_RESOURCES[unsubscriber_key].remove(resource_key)


def notify_subscribers(notifier: Resource, event_time: float):
    resource_key = _resource_key(notifier)
    subscribers = _RESOURCE_SUBSCRIBERS[resource_key]
    if not subscribers:
        logger.info(f"{notifier} has no subscribers")
        return

    active_subscribers = [
        _SUBSCRIPTION_QUEUES[subscriber]
        for subscriber in subscribers
        if subscriber in _SUBSCRIPTION_QUEUES
    ]

    if not active_subscribers:
        logger.info(f"{notifier} has no active subscribers")
        return

    logger.info(f"{notifier}:{event_time} notifying to {subscribers}")

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
    resource_key = _resource_key(resource)

    return _RESOURCE_SUBSCRIBERS[resource_key]


def kill_resource(resource: Resource) -> RegistryQueue | None:
    resource_key = _resource_key(resource)
    if resource_key not in _SUBSCRIPTION_QUEUES:
        return None

    _kill_resource(resource_key)


def deregister(deregisterer: Resource, deregistered_at: float):
    deregisterer_key = _resource_key(deregisterer)

    # This resource is no longer following any resources.
    subscribe_only_to(subscriber=deregisterer, resources=[])

    # Remove this resource's subscription queue
    if deregisterer_key in _SUBSCRIPTION_QUEUES:
        queue = _kill_resource(resource_key=deregisterer_key)
        assert queue  # Just for the type-checker

        del _SUBSCRIPTION_QUEUES[deregisterer_key]

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


class _ResourceKey(NamedTuple):
    resource_type: str
    name: str
    namespace: str | None = None


def _resource_key(resource: Resource) -> _ResourceKey:
    return _ResourceKey(
        resource_type=resource.resource_type.__qualname__,
        name=resource.name,
        namespace=resource.namespace,
    )


def _kill_resource(resource_key: _ResourceKey) -> RegistryQueue | None:
    queue = _SUBSCRIPTION_QUEUES[resource_key]
    try:
        queue.put_nowait(Kill())

    except asyncio.QueueShutDown:
        return queue

    except asyncio.QueueFull:
        pass

    queue.shutdown()

    return queue


def _check_for_cycles(
    subscriber_key: _ResourceKey, resource_keys: Sequence[_ResourceKey]
):
    # Simple, inefficient cycle detection. This is a simple brute-force check,
    # which hopefully given the problem space is sufficient.
    checked: set[_ResourceKey] = set()
    to_check: set[_ResourceKey] = set(resource_keys)
    while True:
        if not to_check:
            break

        if subscriber_key in to_check:
            raise SubscriptionCycle(
                f"Detected subscription cycle due to {subscriber_key}"
            )

        checked.update(to_check)

        next_check_set = set[_ResourceKey]()
        for check_resource_key in to_check:
            if check_resource_key not in _SUBSCRIBER_RESOURCES:
                continue

            check_resource_subscriptions = _SUBSCRIBER_RESOURCES[check_resource_key]
            next_check_set.update(check_resource_subscriptions)
        to_check = next_check_set


_RESOURCE_SUBSCRIBERS: defaultdict[_ResourceKey, set[_ResourceKey]] = defaultdict(
    set[_ResourceKey]
)
_SUBSCRIBER_RESOURCES: defaultdict[_ResourceKey, set[_ResourceKey]] = defaultdict(
    set[_ResourceKey]
)
_SUBSCRIPTION_QUEUES: dict[_ResourceKey, RegistryQueue] = {}


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
