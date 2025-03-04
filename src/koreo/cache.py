from typing import Awaitable, Callable, NamedTuple, Sequence, TypeVar
import asyncio
import copy
import logging
import time

from koreo.result import UnwrappedOutcome, is_unwrapped_ok
from koreo import registry

from constants import ACTIVE_LABEL

logger = logging.getLogger(name="koreo.cache")

T = TypeVar("T")
type PreparerFn[T] = Callable[
    [str, dict],
    Awaitable[UnwrappedOutcome[tuple[T, Sequence[registry.Resource] | None]]],
]


def get_resource_from_cache(
    resource_class: type[T], cache_key: str
) -> UnwrappedOutcome[T] | None:
    resource_class_name = resource_class.__qualname__
    if not __CACHE.get(resource_class_name):
        __CACHE[resource_class_name] = {}

    cached = __CACHE[resource_class_name].get(cache_key)

    if cached:
        return cached.resource

    return None


class __CachedResource[T](NamedTuple):
    spec: dict
    resource: UnwrappedOutcome[T]
    resource_version: str
    prepared_at: float | None = None
    system_data: dict | None = None


def get_resource_system_data_from_cache(
    resource_class: type[T], cache_key: str
) -> __CachedResource[T] | None:
    resource_class_name = resource_class.__qualname__
    if not __CACHE.get(resource_class_name):
        __CACHE[resource_class_name] = {}

    return __CACHE[resource_class_name].get(cache_key)


async def prepare_and_cache(
    resource_class: type[T],
    preparer: PreparerFn[T],
    metadata: dict,
    spec: dict,
    _system_data: dict | None = None,
) -> UnwrappedOutcome[T]:
    resource_metadata = _extract_meta(metadata=metadata)

    cache_key = resource_metadata.resource_name

    resource_class_name = resource_class.__qualname__

    if not __CACHE.get(resource_class_name):
        __CACHE[resource_class_name] = {}

    cached = __CACHE[resource_class_name].get(cache_key)

    if cached and cached.resource_version == resource_metadata.resource_version:
        return cached.resource

    prepare_started_at = time.monotonic()
    resource = registry.Resource(resource_type=resource_class, name=cache_key)
    registry.register(registerer=resource)
    preparer_outcome = await preparer(cache_key, copy.deepcopy(spec))
    prepare_finished_at = time.monotonic()

    if is_unwrapped_ok(preparer_outcome):
        prepared_resource, subscriptions = preparer_outcome
    else:
        prepared_resource = preparer_outcome
        subscriptions = None

    __CACHE[resource_class_name][cache_key] = __CachedResource[T](
        spec=spec,
        resource=prepared_resource,
        resource_version=resource_metadata.resource_version,
        prepared_at=prepare_started_at,
        system_data=_system_data,
    )
    logger.debug(
        f"Updating {resource_class_name} cache for {cache_key} ({resource_metadata.resource_version})."
    )

    await _handle_notifications(
        resource=resource,
        preparer=preparer,
        subscriptions=subscriptions,
        prepare_started_at=prepare_started_at,
        prepare_finished_at=prepare_finished_at,
    )

    return prepared_resource


async def delete_resource_from_cache(
    resource_class: type[T],
    metadata: dict,
) -> None:
    resource_metadata = _extract_meta(metadata=metadata)
    cache_key = resource_metadata.resource_name

    await delete_from_cache(
        resource_class=resource_class,
        cache_key=cache_key,
    )


async def delete_from_cache(
    resource_class: type[T], cache_key: str, version: str | None = None
) -> None:
    resource_class_name = resource_class.__qualname__
    if not __CACHE.get(resource_class_name):
        return None

    cached = __CACHE[resource_class_name].get(cache_key)
    if not cached:
        return None

    if version and version != cached.resource_version:
        logger.debug(
            f"Skip deleting {cache_key} from {resource_class_name} cache due to version mismatch ({version} != {cached.resource_version})."
        )
        return None

    deleted_at = time.monotonic()
    del __CACHE[resource_class_name][cache_key]

    resource = registry.Resource(resource_type=resource_class, name=cache_key)
    queue = registry.kill_resource(resource=resource)
    registry.deregister(deregisterer=resource, deregistered_at=deleted_at)

    # This shouldn't happen, just an extra check
    if resource in _REPREPARE_TASKS:
        _REPREPARE_TASKS[resource].cancel()

    match queue:
        case None:
            pass
        case asyncio.Queue():
            async with asyncio.timeout(5):
                try:
                    await queue.join()
                except asyncio.TimeoutError:
                    pass

    logger.debug(f"Deleted {cache_key} from {resource_class_name} cache.")

    return None


_PREPARE_TIMES: dict[registry.Resource, float] = {}


async def _handle_notifications(
    resource: registry.Resource[T],
    subscriptions: Sequence[registry.Resource] | None,
    prepare_started_at: float,
    prepare_finished_at: float,
    preparer: PreparerFn[T] | None = None,
):
    # We need to track the earliest possible time of prepare.
    _PREPARE_TIMES[resource] = prepare_started_at

    if not subscriptions:
        subscriptions = []

    registry.subscribe_only_to(subscriber=resource, resources=subscriptions)

    # We need to send the latest possible time to subscribers.
    registry.notify_subscribers(notifier=resource, event_time=prepare_finished_at)

    # If this resource watches other resources, setup the monitor to reprepare
    if subscriptions and preparer and resource not in _REPREPARE_TASKS:
        task_name = f"{resource.resource_type.__qualname__}:{resource.name}"
        resource_task = asyncio.create_task(
            _monitor_and_reprepare(
                resource=resource,
                preparer=preparer,
            ),
            name=task_name,
        )
        _REPREPARE_TASKS[resource] = resource_task
        resource_task.add_done_callback(_deletor(resource))


async def _monitor_and_reprepare(
    resource: registry.Resource[T],
    preparer: PreparerFn[T],
):
    queue = registry.register(registerer=resource)
    caught_exception = None
    while True:
        try:
            event = await queue.get()
        except (asyncio.CancelledError, asyncio.QueueShutDown):
            break

        try:
            match event:
                case registry.Kill():
                    break
                case (_, event_time) if event_time <= _PREPARE_TIMES[resource]:
                    continue
                case (_, event_time):
                    try:
                        await _reprepare_and_update_cache(
                            resource_class=resource.resource_type,
                            cache_key=resource.name,
                            preparer=preparer,
                        )
                    except Exception as err:
                        logger.error(
                            f"Critical error preparing {resource.name} "
                            f"({resource.resource_type.__qualname__}) {err}"
                        )
                        caught_exception = err
                        break

        finally:
            queue.task_done()

    if caught_exception:
        raise caught_exception


async def _reprepare_and_update_cache(
    resource_class: type[T],
    preparer: PreparerFn[T],
    cache_key: str,
) -> None:
    resource_class_name = resource_class.__qualname__

    if not __CACHE.get(resource_class_name):
        return

    cached = __CACHE[resource_class_name].get(cache_key)
    if not cached:
        return

    prepare_started_at = time.monotonic()
    preparer_outcome = await preparer(cache_key, copy.deepcopy(cached.spec))
    prepare_finished_at = time.monotonic()

    if is_unwrapped_ok(preparer_outcome):
        prepared_resource, subscriptions = preparer_outcome
    else:
        prepared_resource = preparer_outcome
        subscriptions = None

    __CACHE[resource_class_name][cache_key] = cached._replace(
        resource=prepared_resource, prepared_at=prepare_started_at
    )

    logger.debug(
        f"Repreparing {cache_key} ({cached.resource_version}) in {resource_class_name} cache."
    )

    resource = registry.Resource(resource_type=resource_class, name=cache_key)
    await _handle_notifications(
        resource=resource,
        subscriptions=subscriptions,
        prepare_started_at=prepare_started_at,
        prepare_finished_at=prepare_finished_at,
    )


_REPREPARE_TASKS: dict[registry.Resource, asyncio.Task] = {}


def _deletor(resource):
    def do_delete(task: asyncio.Task):
        del _REPREPARE_TASKS[resource]
        registry.deregister(resource, time.monotonic())

        task_exception = task.exception()
        if task_exception:
            logger.error(f"Re-preparer for {resource} failed with {task_exception}")
            print(f"Re-preparer for {resource} failed with {task_exception}")

    return do_delete


__CACHE: dict[str, dict[str, __CachedResource]] = {}


class __ResourceMetadata(NamedTuple):
    resource_name: str
    resource_version: str

    active: bool


def _extract_meta(metadata: dict) -> __ResourceMetadata:
    resource_name = metadata.get("name")
    resource_version = metadata.get("resourceVersion")

    if not (resource_name and resource_version):
        raise TypeError("Bad Resource: resource name and version are required.")

    labels = metadata.get("labels", {})

    label_active = labels.get(ACTIVE_LABEL, "true").lower() in (
        "t",
        "true",
    )

    return __ResourceMetadata(
        resource_name=resource_name,
        resource_version=resource_version,
        active=label_active,
    )


def _reset_cache():
    """This is for unit testing."""
    __CACHE.clear()

    for task in _REPREPARE_TASKS.values():
        task.cancel()

    registry._reset_registries()
