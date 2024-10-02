import logging
from typing import Awaitable, Callable, NamedTuple


LABEL_NAMESPACE = "koreo.realkinetic.com"

ACTIVE_LABEL = f"{LABEL_NAMESPACE}/active"


def get_resource_from_cache[T](resource_class: type[T], cache_key: str) -> T | None:
    resource_class_name = resource_class.__name__
    if not __CACHE.get(resource_class_name):
        __CACHE[resource_class_name] = {}

    cached = __CACHE[resource_class.__name__].get(cache_key)

    if cached:
        return cached.resource

    return None


async def prepare_and_cache[
    T
](
    resource_class: type[T],
    preparer: Callable[[str, dict], Awaitable[T]],
    metadata: dict,
    spec: dict,
) -> T:
    resource_metadata = _extract_meta(metadata=metadata)

    cache_key = resource_metadata.resource_name

    resource_class_name = resource_class.__name__

    if not __CACHE.get(resource_class_name):
        __CACHE[resource_class_name] = {}

    cached = __CACHE[resource_class_name].get(cache_key)

    if cached and cached.resource_version == resource_metadata.resource_version:
        return cached.resource

    prepared = await preparer(cache_key, spec)

    __CACHE[resource_class_name][cache_key] = __CachedResource[T](
        spec=spec,
        resource=prepared,
        resource_version=resource_metadata.resource_version,
    )
    logging.debug(
        f"Updating {resource_class_name} cache for {cache_key} ({resource_metadata.resource_version})."
    )

    return prepared


async def reprepare_and_update_cache[
    T
](
    resource_class: type[T],
    preparer: Callable[[str, dict], Awaitable[T]],
    cache_key: str,
) -> (T | None):
    resource_class_name = resource_class.__name__

    if not __CACHE.get(resource_class_name):
        return None

    cached = __CACHE[resource_class_name].get(cache_key)
    if not cached:
        return None

    prepared = await preparer(cache_key, cached.spec)

    __CACHE[resource_class_name][cache_key] = __CachedResource[T](
        spec=cached.spec,
        resource=prepared,
        resource_version=cached.resource_version,
    )

    logging.debug(
        f"Repreparing {cache_key} ({cached.resource_version}) in {resource_class_name} cache."
    )

    return prepared


class __CachedResource[T](NamedTuple):
    spec: dict
    resource: T
    resource_version: str


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
    global __CACHE
    __CACHE = {}
