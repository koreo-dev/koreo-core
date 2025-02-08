from typing import NamedTuple, Sequence
import asyncio
import copy

import kr8s

import celpy
from celpy import celtypes

from resources.k8s.conditions import Condition

from koreo import result
from koreo.cel.evaluation import evaluate
from koreo.resource_function.reconcile import reconcile_resource_function
from koreo.value_function.reconcile import reconcile_value_function

from . import structure

# TODO: What is reasonable here? Perhaps 10 seconds?
STEP_TIMEOUT = 10
TIMEOUT_RETRY_DELAY = 30
UNKNOWN_ERROR_RETRY_DELAY = 60


ResourceIds = dict | list["ResourceIds"] | None


class Result(NamedTuple):
    result: result.UnwrappedOutcome[celtypes.Value]
    conditions: list[Condition]
    resource_ids: dict[str, ResourceIds]
    state: celtypes.MapType
    state_errors: dict[str, str]


async def reconcile_workflow(
    api: kr8s.Api,
    workflow_key: str,
    owner: tuple[str, dict],
    trigger: celtypes.Value,
    workflow: structure.Workflow,
) -> Result:
    # This should block no-steps and any non-ok steps.
    # TODO: Make sure to handle resource-ids so we don't cause a problem
    if not result.is_ok(workflow.steps_ready):
        updated_outcome = copy.deepcopy(workflow.steps_ready)
        updated_outcome.location = f"{workflow_key}:{updated_outcome.location}"
        condition = _condition_helper(
            condition_type="Ready",
            thing_name=f"Workflow {workflow_key}",
            outcome=updated_outcome,
            workflow_key=workflow_key,
        )
        return Result(
            result=updated_outcome,
            conditions=[condition],
            resource_ids={},
            state=celtypes.MapType({}),
            state_errors={},
        )

    outcomes, conditions, step_state, state_errors = await _reconcile_steps(
        api=api,
        workflow_key=workflow_key,
        config_step=workflow.config_step,
        steps=workflow.steps,
        owner=owner,
        trigger=trigger,
    )

    outcome_results = {key: result.result for key, result in outcomes.items()}

    outcome_resources = {key: result.resource_ids for key, result in outcomes.items()}

    overall_outcome = result.unwrapped_combine(outcomes=outcome_results.values())
    conditions.append(
        _condition_helper(
            condition_type="Ready",
            thing_name=f"Workflow {workflow_key}",
            outcome=overall_outcome,
            workflow_key=workflow_key,
        )
    )

    return Result(
        result=overall_outcome,
        conditions=conditions,
        resource_ids=outcome_resources,
        state=step_state,
        state_errors=state_errors,
    )


class StepResult(NamedTuple):
    result: result.UnwrappedOutcome[celtypes.Value]
    resource_ids: ResourceIds = None


async def _reconcile_config_step(
    api: kr8s.Api,
    workflow_key: str,
    step: structure.ConfigStep | structure.ErrorStep,
    owner: tuple[str, dict],
    trigger: celtypes.Value,
) -> StepResult:
    location = f"{workflow_key}.spec.configStep"

    if isinstance(step, structure.ErrorStep):
        return StepResult(result=step.outcome)

    if not result.is_unwrapped_ok(step.logic):
        return StepResult(result=step.logic)

    if not step.inputs:
        inputs = celtypes.MapType()

    else:
        inputs = evaluate(
            expression=step.inputs,
            inputs={"parent": trigger},
            location=f"{location}.inputs",
        )
        if not result.is_unwrapped_ok(inputs):
            return StepResult(result=inputs)

    if not isinstance(inputs, celtypes.MapType):
        return StepResult(
            result=result.PermFail(
                message=f"Bad inputs type {type(inputs)}, expected map-type",
                location=f"{location}.inputs",
            )
        )

    inputs[celtypes.StringType("parent")] = trigger

    return await _reconcile_step_logic(
        api=api,
        workflow_key=workflow_key,
        location=location,
        logic=step.logic,
        owner=owner,
        inputs=inputs,
    )


async def _reconcile_steps(
    api: kr8s.Api,
    workflow_key: str,
    owner: tuple[str, dict],
    trigger: celtypes.Value,
    config_step: structure.ConfigStep | structure.ErrorStep | None,
    steps: Sequence[structure.Step | structure.ErrorStep],
) -> tuple[dict[str, StepResult], list[Condition], celtypes.MapType, dict[str, str]]:
    if not (config_step or steps):
        return {}, [], celtypes.MapType({}), {}

    outcome_map: dict[
        str, tuple[structure.StepConditionSpec | None, celpy.Runner | None]
    ] = {}
    task_map: dict[str, asyncio.Task[StepResult]] = {}

    try:
        async with asyncio.timeout(STEP_TIMEOUT), asyncio.TaskGroup() as task_group:
            if config_step:
                task_map[config_step.label] = task_group.create_task(
                    _reconcile_config_step(
                        api=api,
                        workflow_key=workflow_key,
                        step=config_step,
                        owner=owner,
                        trigger=trigger,
                    ),
                    name=config_step.label,
                )
                outcome_map[config_step.label] = (
                    config_step.condition,
                    config_step.state,
                )

            if steps:
                for step in steps:
                    match step:
                        case structure.Step(dynamic_input_keys=dynamic_input_keys):
                            step_dependencies = [
                                task_map[dependency]
                                for dependency in dynamic_input_keys
                            ]

                        case structure.ErrorStep():
                            # TODO: Could probably short-circuit here and set step.outcome?
                            step_dependencies = []

                    task_map[step.label] = task_group.create_task(
                        _reconcile_step(
                            api=api,
                            workflow_key=workflow_key,
                            step=step,
                            owner=owner,
                            dependencies=step_dependencies,
                        ),
                        name=step.label,
                    )
                    outcome_map[step.label] = (step.condition, step.state)
    except:
        # Exceptions will be processed for each task
        pass

    outcomes: dict[str, StepResult] = {}
    conditions = []
    state = celtypes.MapType({})
    state_errors: dict[str, str] = {}

    for task in task_map.values():
        task_name = task.get_name()

        if task.cancelled():
            timeout_outcome = StepResult(
                result=result.Retry(
                    message=f"Timeout running step ({task_name}), will retry.",
                    delay=TIMEOUT_RETRY_DELAY,
                    location=workflow_key,
                )
            )
            outcomes[task_name] = timeout_outcome
            conditions.append(
                _condition_helper(
                    condition_type="Ready",
                    thing_name=f"Workflow {workflow_key}",
                    outcome=timeout_outcome,
                    workflow_key=workflow_key,
                )
            )

        elif task.exception():
            error_outcome = StepResult(
                result=result.Retry(
                    message=f"Unknown error ({task.exception()}) running Step ({task_name}), will retry.",
                    delay=UNKNOWN_ERROR_RETRY_DELAY,
                    location=workflow_key,
                )
            )
            outcomes[task_name] = error_outcome
            conditions.append(
                _condition_helper(
                    condition_type="Ready",
                    thing_name=f"Workflow {workflow_key}",
                    outcome=error_outcome,
                    workflow_key=workflow_key,
                )
            )

        elif task.done():
            step_result = task.result()
            outcomes[task_name] = step_result
            condition_config, state_runner = outcome_map[task_name]
            if condition_config:
                conditions.append(
                    _condition_helper(
                        condition_type=condition_config.type_,
                        thing_name=condition_config.name,
                        outcome=step_result.result,
                        workflow_key=workflow_key,
                    )
                )
            if state_runner and result.is_unwrapped_ok(step_result.result):
                match evaluate(
                    expression=state_runner,
                    inputs={"value": step_result.result},
                    location=f"{task_name}:state",
                ):
                    case celtypes.MapType() as step_state:
                        state.update(step_state)
                    case result.PermFail() as err:
                        state_errors[task_name] = f"{err.message} at {err.location}"
                    case _ as bad_state:
                        state_errors[task_name] = (
                            f"Invalid state type ({type(bad_state)})"
                        )

    return outcomes, conditions, state, state_errors


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
    step: structure.Step | structure.ErrorStep,
    dependencies: list[asyncio.Task[StepResult]],
    owner: tuple[str, dict],
) -> StepResult:
    location = f"{workflow_key}.spec.steps.{step.label}"

    if isinstance(step, structure.ErrorStep):
        return StepResult(result=step.outcome)

    if not dependencies:
        if not step.inputs:
            inputs = celtypes.MapType()
        else:
            inputs = evaluate(
                expression=step.inputs,
                inputs={},
                location=f"{location}.inputs",
            )
            if not result.is_unwrapped_ok(inputs):
                return StepResult(result=inputs)
        if step.skip_if:
            match evaluate(
                expression=step.skip_if, inputs=inputs, location=f"{location}.skipIf"
            ):
                case result.PermFail() as err:
                    return StepResult(result=err)
                case celtypes.BoolType() as should_skip:
                    if should_skip:
                        return StepResult(
                            result=result.Skip(message="skipped by workflow")
                        )
                case _ as bad_type:
                    return StepResult(
                        result=result.PermFail(
                            message=f"`skipIf` must evaluate to a bool, received '{bad_type}' a {type(bad_type)}",
                            location=f"{location}.skipIf",
                        )
                    )

        if step.for_each:
            return await _for_each_reconciler(
                api=api,
                workflow_key=workflow_key,
                location=location,
                step=step,
                steps=celtypes.MapType(),
                owner=owner,
                inputs=inputs,
            )
        else:
            return await _reconcile_step_logic(
                api=api,
                workflow_key=workflow_key,
                location=location,
                logic=step.logic,
                owner=owner,
                inputs=inputs,
            )

    [resolved, pending] = await asyncio.wait(dependencies)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return StepResult(
            result=result.Retry(
                message=f"Timeout running Workflow Steps ({timed_out_tasks}), will retry.",
                delay=15,
                location=location,
            )
        )

    ok_outcomes = celtypes.MapType()

    for task in resolved:
        step_label = task.get_name()
        step_result = task.result()
        match step_result.result:
            case result.DepSkip(message=skip_message, location=skip_location):
                return StepResult(
                    result=result.DepSkip(
                        f"'{step_label}' is waiting on dependency ({skip_message} at {skip_location}).",
                        location=location,
                    )
                )

            case result.Skip(message=skip_message, location=skip_location):
                return StepResult(
                    result=result.DepSkip(
                        f"'{step_label}' was skipped ({skip_message} at {skip_location}).",
                        location=location,
                    )
                )

            case result.Retry(message=retry_message, location=retry_location):
                return StepResult(
                    result=result.DepSkip(
                        f"'{step_label}' is waiting ({retry_message} at {retry_location}).",
                        location=location,
                    )
                )

            case result.PermFail(message=fail_message, location=fail_location):
                return StepResult(
                    result=result.DepSkip(
                        f"'{step_label}' is in failure state ({fail_message} at {fail_location}).",
                        location=location,
                    )
                )

            case result.Ok(data=data):
                ok_outcomes[celtypes.StringType(step_label)] = data

            case _ as data:
                # This should be an UnwrappedOutcome (Ok)
                ok_outcomes[celtypes.StringType(step_label)] = data

    if not step.inputs:
        inputs = celtypes.MapType()
    else:
        inputs = evaluate(
            expression=step.inputs,
            inputs={"steps": ok_outcomes},
            location=f"{location}.inputs",
        )
        if not result.is_unwrapped_ok(inputs):
            return StepResult(result=inputs)

    if step.skip_if:
        match evaluate(
            expression=step.skip_if,
            inputs={"steps": ok_outcomes},
            location=f"{location}.skipIf",
        ):
            case result.PermFail() as err:
                return StepResult(result=err)
            case celtypes.BoolType() as should_skip:
                if should_skip:
                    return StepResult(result=result.Skip(message="skipped by workflow"))
            case _ as bad_type:
                return StepResult(
                    result=result.PermFail(
                        message=f"`skipIf` must evaluate to a bool, received '{bad_type}' a {type(bad_type)}",
                        location=f"{location}.skipIf",
                    )
                )

    if step.for_each:
        return await _for_each_reconciler(
            api=api,
            workflow_key=workflow_key,
            location=location,
            step=step,
            steps=ok_outcomes,
            owner=owner,
            inputs=inputs,
        )
    else:
        return await _reconcile_step_logic(
            api=api,
            workflow_key=workflow_key,
            location=location,
            logic=step.logic,
            owner=owner,
            inputs=inputs,
        )


async def _reconcile_step_logic(
    api: kr8s.Api,
    workflow_key: str,
    owner: tuple[str, dict],
    inputs: celtypes.Value,
    location: str,
    logic: (
        structure.ResourceFunction
        | structure.ValueFunction
        | structure.Workflow
        | result.NonOkOutcome
    ),
) -> StepResult:
    match logic:
        case structure.Workflow():
            workflow_result = await reconcile_workflow(
                api=api,
                workflow_key=workflow_key,
                owner=owner,
                trigger=inputs,
                workflow=logic,
            )
            if result.is_unwrapped_ok(workflow_result.result):
                return StepResult(
                    result=workflow_result.state,
                    resource_ids=workflow_result.resource_ids,
                )

            return StepResult(
                result=workflow_result.result, resource_ids=workflow_result.resource_ids
            )

        case structure.ResourceFunction():
            func_result, resource_id = await reconcile_resource_function(
                api=api,
                location=location,
                function=logic,
                owner=owner,
                inputs=inputs,
            )
            return StepResult(result=func_result, resource_ids=resource_id)
        case structure.ValueFunction():
            return StepResult(
                result=await reconcile_value_function(
                    location=location,
                    function=logic,
                    inputs=inputs,
                )
            )

    return StepResult(result=logic)


async def _for_each_reconciler(
    api: kr8s.Api,
    step: structure.Step,
    workflow_key: str,
    location: str,
    steps: celtypes.MapType,
    owner: tuple[str, dict],
    inputs: celtypes.Value,
) -> StepResult:
    assert step.for_each

    match evaluate(
        expression=step.for_each.source_iterator,
        inputs={"steps": steps},
        location=f"{step.label}.forEach.itemIn",
    ):
        case result.PermFail() as failure:
            return StepResult(result=failure)

        case celtypes.ListType() as source_iterator:
            if not source_iterator:
                return StepResult(result=celtypes.ListType())

        case _ as bad_type:
            return StepResult(
                result=result.PermFail(
                    message=f"Step `{location}.forEach.itemIn` must be a list-type, received {type(bad_type)}.",
                    location=location,
                )
            )

    tasks: list[asyncio.Task[StepResult]] = []

    async with asyncio.TaskGroup() as task_group:
        for idx, map_value in enumerate(source_iterator):
            iterated_inputs = copy.deepcopy(inputs)
            iterated_inputs[step.for_each.input_key] = map_value
            tasks.append(
                task_group.create_task(
                    _reconcile_step_logic(
                        api=api,
                        workflow_key=workflow_key,
                        location=f"{location}[{idx}]",
                        logic=step.logic,
                        owner=owner,
                        inputs=iterated_inputs,
                    ),
                    name=f"{step.label}-{idx}",
                )
            )

    done, pending = await asyncio.wait(tasks)
    if pending:
        timed_out_tasks = ", ".join(task.get_name() for task in pending)
        return StepResult(
            result=result.Retry(
                message=f"Timeout running Workflow For Each Step ({timed_out_tasks}), will retry.",
                delay=15,
                location=location,
            )
        )

    outcomes = [task.result() for task in done]

    error_outcome = result.combine(
        [outcome.result for outcome in outcomes if result.is_error(outcome.result)]
    )
    resource_ids = [outcome.resource_ids for outcome in outcomes]

    if result.is_error(error_outcome):
        return StepResult(result=error_outcome, resource_ids=resource_ids)

    return StepResult(
        result=celtypes.ListType(
            [_outcome_encoder(outcome.result) for outcome in outcomes]
        ),
        resource_ids=resource_ids,
    )


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
