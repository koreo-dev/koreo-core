from typing import Any
import asyncio

import kr8s

from koreo.function.reconcile import reconcile_function

from koreo import result

from . import structure


async def reconcile_workflow(
    api: kr8s.Api,
    trigger: dict,
    workflow: structure.Workflow,
):

    task_map: dict[str, asyncio.Task[result.Outcome]] = {}

    async with asyncio.TaskGroup() as task_group:
        for step in workflow.steps:
            step_dependencies = [
                task_map[dependency] for dependency in step.dynamic_input_keys
            ]
            task_map[step.label] = task_group.create_task(
                _reconcile_step(
                    api=api,
                    step=step,
                    trigger=trigger,
                    dependencies=step_dependencies,
                ),
                name=step.label,
            )

    tasks = task_map.values()
    done, pending = await asyncio.wait(tasks)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return result.Retry(
            message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
            delay=15,
        )

    outcomes = {task.get_name(): task.result() for task in done}

    # TDOO: If not completion specified, just do a simple encoding?
    return {step: _outcome_encoder(outcome) for step, outcome in outcomes.items()}


def _outcome_encoder(outcome: result.Outcome) -> Any:
    if result.is_ok(outcome):
        return outcome.data

    return f"{outcome}"


async def _reconcile_step(
    api: kr8s.Api,
    step: structure.FunctionRef,
    dependencies: list[asyncio.Task[result.Outcome]],
    trigger: dict,
):
    if not dependencies:
        return await reconcile_function(
            api=api,
            function=step.function,
            trigger=trigger,
            inputs=step.static_inputs,
        )

    [resolved, pending] = await asyncio.wait(dependencies)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return result.Retry(
            message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
            delay=15,
        )

    ok_outcomes: dict[str, Any] = {}

    for task in resolved:
        step_label = task.get_name()
        step_result = task.result()
        match step_result:
            case result.Ok(data=data):
                ok_outcomes[step_label] = data
            case _:
                message = step_result.message if hasattr(step_result, "message") else ""
                return result.DepSkip(
                    f"'{step_label}' status is {type(step_result).__name__} ({message})."
                )

    inputs = step.static_inputs | ok_outcomes

    return await reconcile_function(
        api=api,
        function=step.function,
        trigger=trigger,
        inputs=inputs,
    )
