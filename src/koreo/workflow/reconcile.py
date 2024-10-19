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
) -> tuple[result.UnwrappedOutcome[celtypes.Value], list[Condition]]:
    if not result.is_ok(workflow.steps_ready):
        updated_outcome = copy.deepcopy(workflow.steps_ready)
        updated_outcome.location = f"{workflow_key}:{updated_outcome.location}"
        condition = _condition_helper(
            condition_type=f"Ready:{workflow_key}",
            thing_name=f"Workflow {workflow_key}",
            outcome=updated_outcome,
            workflow_key=workflow_key,
        )
        return (updated_outcome, [condition])

    task_map: dict[str, asyncio.Task[result.UnwrappedOutcome]] = {}

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

        timeout_outcome = result.Retry(
            message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
            delay=15,
            location=workflow_key,
        )
        condition = _condition_helper(
            condition_type=f"Ready:{workflow_key}",
            thing_name=f"Workflow {workflow_key}",
            outcome=timeout_outcome,
            workflow_key=workflow_key,
        )
        return (timeout_outcome, [condition])

    outcomes = {task.get_name(): task.result() for task in done}

    conditions = [
        _condition_helper(
            condition_type=step.condition.type_,
            thing_name=step.condition.name,
            outcome=outcomes.get(step.label),
            workflow_key=workflow_key,
        )
        for step in workflow.steps
        if step.condition
    ]

    conditions.extend(
        [
            _condition_helper(
                condition_type=condition_spec.type_,
                thing_name=condition_spec.name,
                outcome=outcomes.get(condition_spec.step),
                workflow_key=workflow_key,
            )
            for condition_spec in workflow.status.conditions
        ]
    )

    overall_outcome = result.unwrapped_combine(outcomes=outcomes.values())
    conditions.append(
        _condition_helper(
            condition_type=f"Ready:{workflow_key}",
            thing_name=f"Workflow {workflow_key}",
            outcome=overall_outcome,
            workflow_key=workflow_key,
        )
    )

    if result.is_error(overall_outcome):
        return (overall_outcome, conditions)

    ok_outcomes = celtypes.MapType(
        {
            celtypes.StringType(step): _outcome_encoder(outcome)
            for step, outcome in outcomes.items()
        }
    )

    if not workflow.status.state:
        return (ok_outcomes, conditions)

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


def _outcome_encoder(outcome: result.UnwrappedOutcome):
    if result.is_error(outcome):
        return outcome

    if result.is_skip(outcome):
        # This will be Skip or DepSkip, which are informational.
        # TODO: Should these be encoded some other way?
        return celtypes.StringType(f"{outcome}")

    # This should be an unwrapped-Ok value
    return outcome


async def _reconcile_step(
    api: kr8s.Api,
    workflow_key: str,
    step: structure.FunctionRef,
    dependencies: list[asyncio.Task[result.UnwrappedOutcome]],
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
            case result.DepSkip(message=skip_message, location=skip_location):
                return result.DepSkip(
                    f"'{step_label}' is waiting on dependency ({skip_message} at {skip_location}).",
                    location=location,
                )

            case result.Skip(message=skip_message, location=skip_location):
                return result.DepSkip(
                    f"'{step_label}' was skipped ({skip_message} at {skip_location}).",
                    location=location,
                )

            case result.Retry(message=retry_message, location=retry_location):
                return result.DepSkip(
                    f"'{step_label}' is waiting ({retry_message} at {retry_location}).",
                    location=location,
                )

            case result.PermFail(message=fail_message, location=fail_location):
                return result.DepSkip(
                    f"'{step_label}' is in failure state ({fail_message} at {fail_location}).",
                    location=location,
                )

            case result.Ok(data=data):
                ok_outcomes[celtypes.StringType(step_label)] = data

            case _:
                # This should be an UnwrappedOutcome (Ok)
                ok_outcomes[celtypes.StringType(step_label)] = step_result

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
) -> result.UnwrappedOutcome[celtypes.ListType]:
    assert step.mapped_input

    source_iterator = step.mapped_input.source_iterator.evaluate({"steps": steps})

    if not isinstance(source_iterator, celtypes.ListType):
        return result.PermFail(
            message=f"Workflow Mapped Step source must be a list-type.",
            location=location,
        )

    tasks: list[asyncio.Task[result.UnwrappedOutcome[celtypes.Value]]] = []

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

    error_outcome = result.combine(
        [outcome for outcome in outcomes if result.is_error(outcome)]
    )

    if result.is_error(error_outcome):
        return error_outcome

    return celtypes.ListType([_outcome_encoder(outcome) for outcome in outcomes])


def _condition_helper(
    condition_type: str,
    thing_name: str,
    outcome: result.UnwrappedOutcome | None,
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

        case result.Ok(location=location):
            reason = "Ready"
            message: str = f"{thing_name} ready."

        case _:
            # This should be an UnwrappedOutcome (Ok)
            reason = "Ready"
            message: str = f"{thing_name} ready."

    return Condition(
        type=condition_type,
        reason=reason,
        message=message,
        status=status,
        location=location,
    )
