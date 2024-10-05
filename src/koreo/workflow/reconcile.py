import asyncio
import copy

import kr8s

from celpy import celtypes

from resources.k8s.conditions import Condition

from koreo import result

from koreo.function.reconcile import reconcile_function

from . import structure


async def reconcile_workflow(
    api: kr8s.Api,
    workflow_key: str,
    trigger: celtypes.Value,
    workflow: structure.Workflow,
) -> tuple[result.Outcome[celtypes.Value], list[Condition]]:
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
            workflow_key=workflow_key,
        )
        for condition_spec in workflow.status.conditions
    ]

    overall_outcome = result.combine(
        [outcome for outcome in outcomes.values() if result.is_error(outcome)]
    )

    if result.is_error(overall_outcome):
        return (overall_outcome, conditions)

    if not workflow.status.state:
        return (
            {step: _outcome_encoder(outcome) for step, outcome in outcomes.items()},
            conditions,
        )

    ok_outcomes = celtypes.MapType(
        {
            step: outcome.data
            for step, outcome in outcomes.items()
            if result.is_ok(outcome)
        }
    )
    try:
        state = workflow.status.state.evaluate(
            {
                "trigger": trigger,
                "steps": ok_outcomes,
            }
        )
    except Exception as err:
        return (
            result.PermFail(
                f"Error evaluating Workflow ({workflow_key}) state ({err})"
            ),
            conditions,
        )

    return (state, conditions)


def _outcome_encoder(outcome: result.Outcome):
    if result.is_ok(outcome):
        return outcome.data

    if result.is_not_error(outcome):
        # This will be Skip or DepSkip, which are informational.
        return celtypes.StringType(f"{outcome}")

    # Bubble errors up.
    return outcome


async def _reconcile_step(
    api: kr8s.Api,
    workflow_key: str,
    step: structure.FunctionRef,
    dependencies: list[asyncio.Task[result.Outcome]],
    trigger: celtypes.Value,
):
    location = f"{workflow_key}.{step.label}"

    if not dependencies:
        if step.inputs:
            inputs = step.inputs.evaluate({})
        else:
            inputs = celtypes.MapType()

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

    ok_outcomes = celtypes.MapType()

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
        inputs = celtypes.MapType()
    else:
        inputs = step.inputs.evaluate({"steps": ok_outcomes})

    if step.mapped_input:
        return await _reconcile_mapped_function(
            api=api,
            location=location,
            step=step,
            steps=ok_outcomes,
            trigger=trigger,
            inputs=inputs,
        )
    else:
        return await reconcile_function(
            api=api,
            location=location,
            function=step.function,
            trigger=trigger,
            inputs=inputs,
        )


async def _reconcile_mapped_function(
    api: kr8s.Api,
    step: structure.FunctionRef,
    location: str,
    steps: celtypes.MapType,
    inputs: celtypes.Value,
    trigger: celtypes.Value,
):
    assert step.mapped_input

    source_iterator = step.mapped_input.source_iterator.evaluate({"steps": steps})

    tasks: list[asyncio.Task[result.Outcome]] = []

    async with asyncio.TaskGroup() as task_group:
        for idx, map_value in enumerate(source_iterator):
            iterated_inputs = copy.deepcopy(inputs)
            iterated_inputs.update({step.mapped_input.input_key: map_value})
            tasks.append(
                task_group.create_task(
                    reconcile_function(
                        api=api,
                        location=f"{location}[{idx}]",
                        function=step.function,
                        trigger=trigger,
                        inputs=iterated_inputs,
                    ),
                    name=f"{step.label}-{idx}",
                )
            )

    done, pending = await asyncio.wait(tasks)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return result.Retry(
            message=f"Timeout running Workflow Mapped Step ({timed_out_tasks}), will retry.",
            delay=15,
            location=location,
        )

    outcomes = [task.result() for task in done]

    overall_outcome = result.combine(
        [outcome for outcome in outcomes if result.is_error(outcome)]
    )

    if result.is_error(overall_outcome):
        return overall_outcome

    return result.Ok(
        celtypes.ListType(_outcome_encoder(outcome) for outcome in outcomes),
        location=location,
    )


def _condition_helper(
    condition_type: str,
    thing_name: str,
    outcome: result.Outcome | None,
    workflow_key: str,
) -> Condition:
    reason = "Pending"
    message: str = f"Awaiting {thing_name} reconciliation."
    status = "True"
    location = workflow_key

    if not outcome:
        return Condition(
            type=condition_type,
            reason=reason,
            message=message,
            status=status,
            location=location,
        )

    match outcome:
        case result.DepSkip(message=skip_message, location=location):
            reason = "DepSkip"
            message: str = f"{thing_name} awaiting dependencies to be ready."
            if skip_message:
                message: str = (
                    f"{thing_name} awaiting dependencies to be ready ({skip_message})"
                )

        case result.Skip(message=skip_message, location=location):
            reason = "Skip"
            message: str = f"{thing_name} management is disabled."
            if skip_message:
                message: str = f"Skipping {thing_name}: {skip_message}"

        case result.Ok(location=location):
            reason = "Ready"
            message: str = f"{thing_name} ready."

        case result.Retry(message=retry_message, location=location):
            reason = "Wait"
            message: str = f"Awaiting {thing_name} to be ready."
            if retry_message:
                message: str = f"Awaiting {thing_name} to be ready ({retry_message})."

        case result.PermFail(message=fail_message, location=location):
            reason = "Failure"
            message: str = (
                f"Unrecoverable error reconciling {thing_name}. ({fail_message})."
            )

    return Condition(
        type=condition_type,
        reason=reason,
        message=message,
        status=status,
        location=location,
    )
