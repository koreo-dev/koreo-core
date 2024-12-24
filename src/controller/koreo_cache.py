import asyncio
import logging

import kr8s

from koreo.cache import delete_resource_from_cache, prepare_and_cache


async def load_cache(
    namespace: str,
    api_version: str,
    plural_kind: str,
    kind_title: str,
    resource_class,
    preparer,
):
    logging.info(f"Building initial {plural_kind}.{api_version} cache.")

    resource_class = kr8s.objects.new_class(
        version=api_version,
        kind=kind_title,
        plural=plural_kind,
        namespaced=True,
        scalable=False,
        asyncio=True,
    )
    resources = resource_class.list(namespace=namespace)

    for resource in await resources:
        logging.debug(f"Caching {resource.name}.")
        await prepare_and_cache(
            resource_class=resource_class,
            preparer=preparer,
            metadata=resource.metadata,
            spec=resource.raw.get("spec", {}),
        )

    logging.debug(f"Initial {plural_kind}.{api_version} cache load complete.")


async def maintain_cache(
    namespace: str,
    api_version: str,
    plural_kind: str,
    kind_title: str,
    resource_class,
    preparer,
):
    logging.debug(f"Maintaining {plural_kind}.{api_version} Cache.")

    while True:
        try:
            kr8s_api = kr8s.api()
            kr8s_api.timeout = 600

            resource_class = kr8s.objects.new_class(
                version=api_version,
                kind=kind_title,
                plural=plural_kind,
                namespaced=True,
                scalable=False,
                asyncio=True,
            )

            watcher = kr8s_api.async_watch(kind=resource_class, namespace=namespace)

            async for event, resource in watcher:
                if event == "DELETED":
                    logging.debug(
                        f"Deleting {plural_kind}.{api_version} from cache due to {event} for {resource.name}."
                    )
                    await delete_resource_from_cache(
                        resource_class=resource_class,
                        metadata=resource.metadata,
                    )
                    continue

                logging.debug(
                    f"Updating {plural_kind}.{api_version} cache due to {event} for {resource.name}."
                )
                await prepare_and_cache(
                    resource_class=resource_class,
                    preparer=preparer,
                    metadata=resource.metadata,
                    spec=resource.raw.get("spec"),
                )
        except:
            logging.exception(
                f"Restarting {plural_kind}.{api_version} cache maintainer watch."
            )
            # NOTE: This is just to prevent completely blowing up the API
            # Server if there's an issue. It probably should have a back-off
            # based on the last retry time.

            await asyncio.sleep(30)
