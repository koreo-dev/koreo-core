from typing import NamedTuple
import copy
import time

from koreo import cache
from koreo import registry
from koreo.result import is_ok, is_unwrapped_ok
from koreo.workflow.structure import Workflow


class Status(NamedTuple):
    resource_type: str
    cache_key: str
    resource_version: str
    prepared_ago_seconds: str
    resource_status: str
    subscriptions: list
    subscribers: list


def _get_resource_status_for_type(resource):
    if not is_unwrapped_ok(resource):
        return f"{resource}"

    match resource:
        case Workflow(steps_ready=steps_ready):
            if is_ok(steps_ready):
                return "Ok"

            return f"Steps not ready due to {steps_ready}"

        case _:
            return "Ok"


def list_resources():
    request_time = time.monotonic()

    cached_resources: list[Status] = []
    for resource_key, cached_resource in cache.__CACHE.items():
        subscribers = registry.get_subscribers(resource_key)
        subscriptions = registry.get_subscriptions(resource_key)

        prepared_ago_seconds = "never"
        if cached_resource.prepared_at:
            age = request_time - cached_resource.prepared_at
            prepared_ago_seconds = f"{age:.4f}"

        cached_resources.append(
            Status(
                resource_type=resource_key.resource_type.__qualname__,
                cache_key=resource_key.name,
                resource_version=cached_resource.resource_version,
                prepared_ago_seconds=prepared_ago_seconds,
                resource_status=_get_resource_status_for_type(cached_resource.resource),
                subscriptions=[
                    copy.replace(
                        subscription,
                        resource_type=subscription.resource_type.__qualname__,
                    )
                    for subscription in subscriptions
                ],
                subscribers=[
                    copy.replace(
                        subscriber,
                        resource_type=subscriber.resource_type.__qualname__,
                    )
                    for subscriber in subscribers
                ],
            )
        )

    return cached_resources
