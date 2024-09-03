from collections import defaultdict
import logging
from typing import Callable, NamedTuple
import re


LABEL_NAMESPACE = "koreo.realkinetic.com"

NAME_LABEL = f"{LABEL_NAMESPACE}/name"
VERSION_LABEL = f"{LABEL_NAMESPACE}/version"
ACTIVE_LABEL = f"{LABEL_NAMESPACE}/active"


def build_cache_key(name: str, version: str) -> str:
    return f"{name}:{version}"


def get_resource_from_cache[T](resource_type: type[T], cache_key: str) -> T | None:
    cached = __CACHE[T.__name__].get(cache_key)

    if cached:
        return cached.resource

    return None


def get_spec_from_cache[T](resource_type: type[T], cache_key: str) -> dict | None:
    cached = __CACHE[T.__name__].get(cache_key)

    if cached:
        return cached.spec

    return None


async def prepare_and_cache[
    T
](preparer: Callable[[dict], T], metadata: dict, spec: dict) -> T:
    resource_metadata = _extract_meta(metadata=metadata)

    cache_key = build_cache_key(
        name=resource_metadata.name, version=resource_metadata.version
    )

    if not __CACHE.get(T.__name__):
        __CACHE[T.__name__] = {}

    cached = __CACHE[T.__name__].get(cache_key)

    if cached and cached.resource_version == resource_metadata.resource_version:
        return cached.resource

    logging.info("Before prepare")
    prepared = await preparer(cache_key=cache_key, spec=spec)
    logging.info("Post prepare")

    __CACHE[T.__name__][cache_key] = __CachedResource[T](
        spec=spec,
        resource=prepared,
        resource_version=resource_metadata.resource_version,
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

    name: str
    version: str
    active: bool


def _extract_meta(metadata: dict) -> __ResourceMetadata:
    resource_name = metadata.get("name")
    resource_version = metadata.get("resourceVersion")

    if not (resource_name and resource_version):
        raise Exception("Bad Resource: resource name and version are required.")

    labels = metadata.get("labels", {})

    label_name = labels.get(NAME_LABEL)
    label_version = labels.get(VERSION_LABEL, "").lower()

    if not label_name or not _valid_version(label_version):
        raise Exception(
            f"Bad label configuration, name and version labels are required for {resource_name}"
        )

    label_active = labels.get(ACTIVE_LABEL, "true").lower() in (
        "t",
        "true",
    )

    return __ResourceMetadata(
        resource_name=resource_name,
        resource_version=resource_version,
        name=label_name,
        version=label_version,
        active=label_active,
    )


_version_checker = re.compile(r"v\d+[a-z]*\d*")


def _valid_version(version: str):
    return version == "latest" or _version_checker.match(version)
