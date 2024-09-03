import asyncio
import logging

logging.basicConfig(format="%(name)s\t:%(levelname)s: %(message)s", level=logging.DEBUG)

import os

import uvloop

import kopf

from controller import koreo_cache

from koreo.function.prepare import prepare_function
from koreo.workflow.prepare import prepare_workflow


GROUP = "koreo.realkinetic.com"
VERSION = "v1alpha8"
API_VERSION = f"{GROUP}/{VERSION}"

KOREO_RESOURCES = [
    ("functions", "Function", prepare_function),
    ("workflows", "Workflow", prepare_workflow),
]

KOREO_NAMESPACE = os.environ.get("KOREO_NAMESPACE", "koreo-testing")

RESOURCE_NAMESPACE = os.environ.get("RESOURCE_NAMESPACE", "koreo-testing")


def start_custom_workflow_controller():
    import controller.custom_workflow


__tasks = set()


def main():

    loop = uvloop.EventLoopPolicy().new_event_loop()

    # First load the Functions, then Workflows to ensure they're cached.
    # Then maintain the Function and Workflow caches in the background.

    for plural_kind, kind_title, preparer in KOREO_RESOURCES:
        # Block until completion.
        load_task = loop.create_task(
            koreo_cache.load_cache(
                koreo_namespace=KOREO_NAMESPACE,
                api_version=API_VERSION,
                plural_kind=plural_kind,
                kind_title=kind_title,
                preparer=preparer,
            )
        )
        __tasks.add(load_task)
        load_task.add_done_callback(__tasks.discard)

        loop.run_until_complete(load_task)

        # Spawn in backgound
        maintain_task = loop.create_task(
            koreo_cache.maintain_cache(
                koreo_namespace=KOREO_NAMESPACE,
                api_version=API_VERSION,
                plural_kind=plural_kind,
                kind_title=kind_title,
                preparer=preparer,
            )
        )
        __tasks.add(maintain_task)
        maintain_task.add_done_callback(__tasks.discard)

    # Fire up the kopf situation.
    start_custom_workflow_controller()

    try:
        loop.run_until_complete(kopf.operator(namespace=RESOURCE_NAMESPACE))
    except:
        tasks = [
            task
            for task in asyncio.tasks.all_tasks(loop=loop)
            if task is not asyncio.tasks.current_task()
        ]
        for task in tasks:
            task.cancel()
        asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()
        exit()


if __name__ == "__main__":
    main()
