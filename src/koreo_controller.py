import asyncio
import logging

logging.basicConfig(format="%(name)s\t:%(levelname)s: %(message)s", level=logging.DEBUG)

import os

import uvloop

import kopf

from controller import koreo_cache

from koreo import registry
from koreo.resource_function.prepare import prepare_resource_function
from koreo.resource_function.structure import ResourceFunction
from koreo.resource_template.prepare import prepare_resource_template
from koreo.resource_template.structure import ResourceTemplate
from koreo.value_function.prepare import prepare_value_function
from koreo.value_function.structure import ValueFunction
from koreo.workflow.prepare import prepare_workflow
from koreo.workflow.structure import Workflow


GROUP = "koreo.realkinetic.com"
VERSION = "v1alpha8"
API_VERSION = f"{GROUP}/{VERSION}"

KOREO_NAMESPACE = os.environ.get("KOREO_NAMESPACE", "koreo-testing")

TEMPLATE_NAMESPACE = os.environ.get("TEMPLATE_NAMESPACE", "koreo-testing")

RESOURCE_NAMESPACE = os.environ.get("RESOURCE_NAMESPACE", "koreo-testing")

KOREO_RESOURCES = [
    (
        TEMPLATE_NAMESPACE,
        "ResourceTemplate",
        ResourceTemplate,
        prepare_resource_template,
    ),
    (KOREO_NAMESPACE, "ValueFunction", ValueFunction, prepare_value_function),
    (KOREO_NAMESPACE, "ResourceFunction", ResourceFunction, prepare_resource_function),
    (KOREO_NAMESPACE, "Workflow", Workflow, prepare_workflow),
]


def start_custom_workflow_controller():
    import controller.custom_workflow


__tasks = set()


def main():

    loop = uvloop.EventLoopPolicy().new_event_loop()

    # First load the Functions, then Workflows to ensure they're cached.
    # Then maintain the Function and Workflow caches in the background.

    for namespace, kind_title, resource_class, preparer in KOREO_RESOURCES:
        # Block until completion.
        load_task = loop.create_task(
            koreo_cache.load_cache(
                namespace=namespace,
                api_version=API_VERSION,
                plural_kind=f"{kind_title.lower()}s",
                kind_title=kind_title,
                resource_class=resource_class,
                preparer=preparer,
            )
        )
        __tasks.add(load_task)
        load_task.add_done_callback(__tasks.discard)

        loop.run_until_complete(load_task)

        # Spawn in backgound
        maintain_task = loop.create_task(
            koreo_cache.maintain_cache(
                namespace=namespace,
                api_version=API_VERSION,
                plural_kind=f"{kind_title.lower()}s",
                kind_title=kind_title,
                resource_class=resource_class,
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
        logging.exception("Shutting down due to exception.")

        for resource_key in registry._SUBSCRIPTION_QUEUES:
            registry._kill_resource(resource_key=resource_key)

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
