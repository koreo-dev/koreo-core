import logging

import kr8s

from koreo.function.reconcile import reconcile_function

from koreo import result

from . import structure


async def reconcile_workflow(api: kr8s.Api, workflow: structure.Workflow):
    accumulated_outputs: dict[str, result.Outcome] = {}

    is_not_ok = False

    for step in workflow.steps:
        if is_not_ok:
            accumulated_outputs[step.label] = result.DepSkip("Prior outcome non-Ok")
            continue

        outcome = await reconcile_function(
            api=api, function=step.function, inputs=accumulated_outputs
        )
        logging.info(f"{outcome}")

        if result.is_not_ok(outcome):
            is_not_ok = True
            accumulated_outputs[step.label] = outcome
            continue

        accumulated_outputs[step.label] = outcome.data

    return accumulated_outputs
