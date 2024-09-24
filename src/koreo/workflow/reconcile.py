from typing import Any
import asyncio
import json

import celpy
import kr8s

from resources.k8s.conditions import Condition

from koreo import result

from koreo.function.reconcile import reconcile_function

from . import structure


async def reconcile_workflow(
    api: kr8s.Api,
    workflow_key: str,
    trigger: dict,
    workflow: structure.Workflow,
) -> tuple[result.Outcome, list[Condition]]:
    if not result.is_ok(workflow.steps_ready):
        return (workflow.steps_ready, [])

    task_map: dict[str, asyncio.Task[result.Outcome]] = {}

    async with asyncio.TaskGroup() as task_group:
        for step in workflow.steps:
            step_dependencies = [
                task_map[dependency] for dependency in step.dynamic_input_keys
            ]
            task_map[step.label] = task_group.create_task(
                _reconcile_step(
                    api=api,
                    workflow_key=workflow_key,
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
        return (
            result.Retry(
                message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
                delay=15,
                location=workflow_key,
            ),
            [],
        )

    outcomes = {task.get_name(): task.result() for task in done}

    conditions = [
        _condition_helper(
            condition_type=condition_spec.type_,
            thing_name=condition_spec.name,
            outcome=outcomes.get(condition_spec.step),
        )
        for condition_spec in workflow.status.conditions
    ]

    # TDOO: If no completion specified, just do a simple encoding?
    overall_result = {
        step: _outcome_encoder(outcome) for step, outcome in outcomes.items()
    }

    overall_outcome = result.combine(
        [outcome for outcome in overall_result.values() if result.is_error(outcome)]
    )

    if result.is_error(overall_outcome):
        return (overall_outcome, conditions)

    return (overall_result, conditions)


def _outcome_encoder(outcome: result.Outcome) -> Any:
    if result.is_ok(outcome):
        return outcome.data

    if result.is_not_error(outcome):
        # This will be Skip or DepSkip, which are informational.
        return f"{outcome}"

    # Bubble errors up.
    return outcome


async def _reconcile_step(
    api: kr8s.Api,
    workflow_key: str,
    step: structure.FunctionRef,
    dependencies: list[asyncio.Task[result.Outcome]],
    trigger: dict,
):
    location = f"{workflow_key}.{step.label}"

    if not dependencies:
        if step.inputs:
            inputs = json.loads(json.dumps(step.inputs.evaluate({})))
        else:
            inputs = {}

        return await reconcile_function(
            api=api,
            location=location,
            function=step.function,
            trigger=trigger,
            inputs=inputs,
        )

    [resolved, pending] = await asyncio.wait(dependencies)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return result.Retry(
            message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
            delay=15,
            location=location,
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
                    f"'{step_label}' status is {type(step_result).__name__} ({message}).",
                    location=location,
                )

    if not step.inputs:
        inputs = {}
    else:
        inputs = json.loads(
            json.dumps(step.inputs.evaluate({"steps": celpy.json_to_cel(ok_outcomes)}))
        )

    return await reconcile_function(
        api=api,
        location=location,
        function=step.function,
        trigger=trigger,
        inputs=inputs,
    )


def _condition_helper(
    condition_type: str, thing_name: str, outcome: result.Outcome | None
) -> Condition:
    reason = "Pending"
    message: str = f"Awaiting {thing_name} reconciliation."
    status = "True"

    if not outcome:
        return Condition(
            type=condition_type,
            reason=reason,
            message=message,
            status=status,
        )

    match outcome:
        case result.DepSkip(message=skip_message, location=location):
            reason = "DepSkip"
            message: str = (
                f"{thing_name} awaiting dependencies to be ready. ({location})"
            )
            if skip_message:
                message: str = (
                    f"{thing_name} awaiting dependencies to be ready ({skip_message})"
                )

        case result.Skip(message=skip_message, location=location):
            reason = "Skip"
            message: str = f"{thing_name} management is disabled. ({location})"
            if skip_message:
                message: str = f"Skipping {thing_name}: {skip_message}"

        case result.Ok(location=location):
            reason = "Ready"
            message: str = f"{thing_name} ready. ({location})"

        case result.Retry(message=retry_message, location=location):
            reason = "Wait"
            message: str = f"Awaiting {thing_name} to be ready. ({location})"
            if retry_message:
                message: str = (
                    f"Awaiting {thing_name} to be ready ({retry_message}). ({location})"
                )

        case result.PermFail(message=fail_message, location=location):
            reason = "Failure"
            message: str = (
                f"Unrecoverable error reconciling {thing_name}. ({fail_message}). ({location})"
            )

    return Condition(
        type=condition_type,
        reason=reason,
        message=message,
        status=status,
    )
