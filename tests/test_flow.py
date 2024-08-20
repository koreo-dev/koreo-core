import unittest
import string
import random

from asyncio import TaskGroup, sleep

from koreo.flow import UnwrapOk, _execute_step, flow_control
from koreo.result import DepSkip, Ok, Outcome, PermFail, Retry
from koreo.workflow import Workflow


ID_CHARS = string.ascii_letters + string.digits


def step_id_generator():
    return "".join(random.choices(ID_CHARS, k=10))


async def independent():
    await sleep(delay=0)
    return Ok("independent")


async def dependent(prior_step: Outcome):
    await sleep(delay=0)
    if isinstance(prior_step, Ok):
        return Ok({"prior_step": prior_step.data})
    else:
        return prior_step


async def retry_work():
    await sleep(delay=0)
    return Retry(delay=30)


async def fail_work():
    await sleep(delay=0)
    return PermFail()


class ActionTest[T]:
    response_type: Outcome[T]

    def __init__(self, response_type: Outcome[T]):
        self.response_type = response_type

    async def action(self, **results: Outcome) -> Outcome[T]:
        await sleep(delay=0)
        return self.response_type


class NeedTester[T]:
    response_type: Outcome[T]

    def __init__(self, response_type: Outcome[T]):
        self.response_type = response_type

    async def action(self, **results: Outcome) -> Outcome[T]:
        await sleep(delay=0)
        return self.response_type


async def complete_work(**results: Outcome) -> Outcome[dict[str, Outcome]]:
    await sleep(delay=0)
    return Ok(results)


class CompletionTest[T]:
    response: Outcome[T]

    def __init__(self, response: Outcome[T]):
        self.response = response

    async def action(self, **results: Outcome) -> Outcome[T]:
        await sleep(delay=0)
        return self.response


class TestFlowControl(unittest.IsolatedAsyncioTestCase):
    async def test_no_steps(self):
        outcome = await flow_control(Workflow(steps={}))
        self.assertIsInstance(outcome, PermFail)
        self.assertIn("one step is required", outcome.message)

    async def test_out_of_order_steps(self):
        outcome = await flow_control(
            Workflow(steps={"dependent_step": dependent, "prior_step": independent})
        )

        self.assertIsInstance(outcome, PermFail)
        self.assertIn("out of order", outcome.message)

    async def test_circular_dep(self):
        outcome = await flow_control(Workflow(steps={"prior_step": dependent}))

        self.assertIsInstance(outcome, PermFail)
        self.assertIn("self-reference", outcome.message)

    async def test_bad_arg_type(self):
        async def random_arg(some_arg: str):
            await sleep(0)
            return Ok(some_arg)

        outcome = await flow_control(
            Workflow(
                steps={
                    "random_arg": random_arg,
                }
            )
        )

        self.assertIsInstance(outcome, PermFail)
        self.assertIn("must be of type", outcome.message)

    async def test_missing_dep(self):
        outcome = await flow_control(
            Workflow(
                steps={
                    "prr_step": independent,
                    "dependent_step": dependent,
                }
            )
        )

        self.assertIsInstance(outcome, PermFail)
        self.assertIn("does not appear", outcome.message)

    async def test_ok_one_independent_step(self):
        step_id = step_id_generator()

        outcome = await flow_control(Workflow(steps={step_id: independent}))

        self.assertIsInstance(outcome, Ok)
        self.assertEqual(outcome.data, "independent")

    async def test_ok_two_independent_steps(self):
        a_step_id = step_id_generator()
        b_step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(steps={a_step_id: independent, b_step_id: independent})
        )

        self.assertIsInstance(outcome, Ok)
        self.assertEqual(outcome.data, ["independent", "independent"])

    async def test_dependent_steps(self):
        dependent_step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(steps={"prior_step": independent, dependent_step_id: dependent})
        )

        self.assertIsInstance(outcome, Ok)

        self.assertIn(
            {"prior_step": "independent"},
            outcome.data,
        )
        self.assertIn(
            "independent",
            outcome.data,
        )

    async def test_retry_step(self):
        step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(
                steps={step_id: retry_work},
            )
        )

        self.assertIsInstance(outcome, Retry)

    async def test_permfail_step(self):
        step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(
                steps={
                    step_id: fail_work,
                }
            )
        )

        self.assertIsInstance(outcome, PermFail)

    async def test_one_fail_one_ok(self):
        a_step_id = step_id_generator()
        b_step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(steps={a_step_id: independent, b_step_id: fail_work})
        )

        self.assertIsInstance(outcome, PermFail)

    async def test_completion_function(self):
        step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(
                steps={
                    step_id: fail_work,
                },
                completion=complete_work,
            )
        )

        self.assertIsInstance(outcome, Ok)
        self.assertIsInstance(outcome.data.get(step_id), PermFail)

    async def test_completion_class(self):
        step_id = step_id_generator()

        outcome = await flow_control(
            Workflow(
                steps={
                    step_id: fail_work,
                },
                completion=CompletionTest(response=Ok("Context")).action,
            )
        )

        self.assertIsInstance(outcome, Ok)
        self.assertEqual(outcome.data, "Context")


class TestExecuteStep(unittest.IsolatedAsyncioTestCase):
    async def test_nothing_needed(self):
        pairs = [
            (independent, Ok),
            (retry_work, Retry),
            (fail_work, PermFail),
            (ActionTest(Ok("test")).action, Ok),
            (ActionTest(Retry(message="retry-test", delay=20)).action, Retry),
            (ActionTest(PermFail(message="perm-fail-test")).action, PermFail),
        ]
        for action, expected_outcome in pairs:
            outcome = await _execute_step(action, needed_tasks=[])

            self.assertIsInstance(outcome, expected_outcome)

    async def test_ok_need(self):
        async with TaskGroup() as task_group:
            needed_tasks = [task_group.create_task(independent(), name="prior_step")]

            outcome = await _execute_step(
                dependent,
                needed_tasks=needed_tasks,  # type: ignore
            )

        self.assertIsInstance(outcome, Ok)

    async def test_unwrap_ok_with_ok_input(self):
        result_value = random.randint(10, 10000)

        async def input():
            await sleep(delay=0)
            return Ok(result_value)

        async def consumer(input: UnwrapOk[int]):
            await sleep(delay=0)
            return Ok(input)

        async with TaskGroup() as task_group:
            needed_tasks = [
                task_group.create_task(input(), name="input"),
            ]

            outcome = await _execute_step(
                consumer,
                needed_tasks=needed_tasks,  # type: ignore
            )

        self.assertIsInstance(outcome, Ok)
        self.assertEqual(outcome.data, result_value)

    async def test_unwrap_ok_with_retry_input(self):
        async def input():
            await sleep(delay=0)
            return Retry(message="Retry outcome!", delay=0)

        async def consumer(input: UnwrapOk[int]):
            await sleep(delay=0)
            return Ok(input)

        async with TaskGroup() as task_group:
            needed_tasks = [
                task_group.create_task(input(), name="input"),
            ]

            outcome = await _execute_step(
                consumer,
                needed_tasks=needed_tasks,  # type: ignore
            )

        self.assertIsInstance(outcome, DepSkip)
        self.assertIn("Retry", outcome.message)
        self.assertIn("Retry outcome!", outcome.message)


#     async def test_wrong_outcome_need(self):
#         async with TaskGroup() as task_group:
#             dep_step_id: str = step_id_generator()
#
#             needed_tasks = [
#                 task_group.create_task(retry_work(need_results={}), name=dep_step_id)
#             ]
#
#             outcome = await execute_step(
#                 ok_work,
#                 needs={
#                     dep_step_id: [
#                         DepSkip,
#                         Skip,
#                         Ok,
#                         # Retry,
#                         PermFail,
#                     ]
#                 },
#                 needed_tasks=needed_tasks,  # type: ignore
#             )
#
#         self.assertIsInstance(outcome, DepSkip)
#
#     async def test_needs_mismatch(self):
#         dep_step_id: str = step_id_generator()
#
#         needed_tasks = []
#
#         outcome = await execute_step(
#             ok_work,
#             needs={
#                 dep_step_id: [
#                     Ok,
#                 ]
#             },
#             needed_tasks=needed_tasks,
#         )
#
#         self.assertIsInstance(outcome, PermFail)
