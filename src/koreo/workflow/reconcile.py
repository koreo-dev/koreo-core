import logging

import kr8s

from koreo.function.reconcile import reconcile_function

from koreo import result

from . import structure


async def reconcile_workflow(
    api: kr8s.Api,
    trigger_metadata: dict,
    trigger_spec: dict,
    workflow: structure.Workflow,
):
    accumulated_outputs: dict[str, result.Outcome] = {}

    is_not_ok = False

    for step in workflow.steps:
        if is_not_ok:
            accumulated_outputs[step.label] = result.DepSkip("Prior outcome non-Ok")
            continue

        outcome = await reconcile_function(
            api=api,
            function=step.function,
            trigger_metadata=trigger_metadata,
            trigger_spec=trigger_spec,
            inputs=accumulated_outputs,
        )
        logging.info(f"{outcome}")

        if result.is_not_ok(outcome):
            is_not_ok = True
            accumulated_outputs[step.label] = outcome
            continue

        accumulated_outputs[step.label] = outcome.data

    if is_not_ok:
        error_outcomes = [
            outcome
            for outcome in accumulated_outputs.values()
            if result.is_error(outcome)
        ]
        return result.combine(error_outcomes)

    return accumulated_outputs
