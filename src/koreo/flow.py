import asyncio
import logging

from typing import Annotated, Mapping, get_origin, get_type_hints, get_args


from .result import DepSkip, Ok, Outcome, PermFail, Retry, Skip, combine
from .workflow_old import ActionFn, Workflow


UNWRAP_OK = "__unwrap_ok__"

type UnwrapOk[T] = Annotated[T, UNWRAP_OK]

# TODO: Add unit-test helper that can run the _lookup_needs / sequence check
# logic as a unit-test to surface these issues at dev time.


async def flow_control(workflow: Workflow):
    if not workflow.steps:
        return PermFail("At least one step is required for a Workflow.")

    task_map: Mapping[str, asyncio.Task[Outcome]] = {}

    async with asyncio.TaskGroup() as task_group:
        for step_id, action_fn in workflow.steps.items():
            try:
                needed_tasks = _lookup_needs(action_fn, task_map)
            except KeyError as e:
                missing_key = e.args[0]
                if missing_key == step_id:
                    message = (
                        f"Step '{step_id}' is creating a circular "
                        "dependency; Steps can not self-reference."
                    )
                elif missing_key not in workflow.steps:
                    message = (
                        f"Step '{step_id}' references '{missing_key}' "
                        "which does not appear in Workflow.steps; perhaps it is a typo."
                    )
                else:
                    message = (
                        f"Steps are specified out of order; Step '{step_id}' "
                        f"must be listed after step '{missing_key}'."
                    )
                return PermFail(message=message)
            except TypeError as e:
                return PermFail(message=str(e))

            task_map[step_id] = task_group.create_task(
                _execute_step(action_fn, needed_tasks),
                name=step_id,
            )

    tasks = task_map.values()
    done, pending = await asyncio.wait(tasks)
    if pending:
        # I'm not sure when this will happen, but I'm sure it will.
        raise Exception("Pending tasks.")

    if not workflow.completion:
        task_results = [task.result() for task in done]
        return combine(task_results)

    task_outcomes = {task.get_name(): task.result() for task in done}
    return await workflow.completion(**task_outcomes)


OUTCOME_BASE_TYPES = [get_origin(t) or t for t in get_args(Outcome)]


def _lookup_needs(
    action_fn: ActionFn, task_map: Mapping[str, asyncio.Task[Outcome]]
) -> list[asyncio.Task[Outcome]] | None:
    arg_hints = {
        name: hint
        for name, hint in get_type_hints(action_fn).items()
        if name != "return"
    }
    if not arg_hints:
        return None

    needed_tasks: list[asyncio.Task[Outcome]] = []
    for param_name, hint in arg_hints.items():
        hint_type_origin = get_origin(hint)
        hint_base_types = [get_origin(t) or t for t in get_args(hint)]
        if not (hint_type_origin is UnwrapOk or OUTCOME_BASE_TYPES == hint_base_types):
            raise TypeError(
                f"Parameter '{param_name}' to '{action_fn.__qualname__}' must be of type `Outcome` or `UnwrapOk`, got `{hint}`"
            )
        needed_tasks.append(task_map[param_name])

    return needed_tasks


async def _execute_step[
    T
](action: ActionFn[T], needed_tasks: list[asyncio.Task[Outcome]] | None) -> Outcome[T]:
    if not needed_tasks:
        return await action()

    [resolved, pending] = await asyncio.wait(needed_tasks)
    if pending:
        # I'm not sure when this will happen, but I'm sure it will.
        raise Exception("Pending results.")

    action_args = {task.get_name(): task.result() for task in resolved}

    arg_hints = {
        name: hint
        for name, hint in get_type_hints(action, include_extras=True).items()
        if name != "return"
    }

    for name, need_hint in arg_hints.items():
        needed_type_origin = get_origin(need_hint)
        if needed_type_origin is UnwrapOk:
            result = action_args.get(name, Skip(f"Missing result for {name}"))

            match result:
                case Ok(data=data):
                    action_args[name] = data
                case _:
                    return DepSkip(
                        f"'{name}' status is {type(result).__name__} ({result.message})."
                    )

    try:
        return await action(**action_args)
    except TypeError as e:
        return PermFail(message=f"{e}")
    except Exception as e:
        logging.exception("Unhandled exception running action")
        return Retry(message=f"Unknown error: {e}", delay=300)
