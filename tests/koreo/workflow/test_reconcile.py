import unittest

import celpy
from celpy import celtypes

from koreo.result import Ok, is_unwrapped_ok

from koreo.cel.prepare import prepare_overlay_expression

from koreo.value_function import structure as function_structure

from koreo.workflow import reconcile
from koreo.workflow import structure as workflow_structure


class TestReconcileWorkflow(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile(self):
        cel_env = celpy.Environment()
        source_return_value = prepare_overlay_expression(
            cel_env=cel_env,
            spec={"resources": [{"bool": True}, {"bool": False}]},
            location="unittest",
        )

        step_state = cel_env.program(cel_env.compile("{'input_source': value}"))

        workflow = workflow_structure.Workflow(
            name="unit-test",
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.dev", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            steps=[
                workflow_structure.Step(
                    label="input_source",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=source_return_value,
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=step_state,
                )
            ],
            dynamic_input_keys=set(),
        )

        workflow_result = await reconcile.reconcile_workflow(
            api=None,
            workflow_key="test-case",
            owner=("unit-tests", celtypes.MapType({"uid": "sam-123"})),
            trigger=celtypes.MapType({}),
            workflow=workflow,
        )

        self.maxDiff = None
        self.assertDictEqual(
            {"input_source": {"resources": [{"bool": True}, {"bool": False}]}},
            workflow_result.state,
        )

        # TODO: Check Condition

    async def test_reconcile_nested(self):
        cel_env = celpy.Environment()

        sub_step_return_value = prepare_overlay_expression(
            cel_env=cel_env, spec={"sub_one": True}, location="unittest:sub_step.return"
        )

        assert is_unwrapped_ok(sub_step_return_value), f"{sub_step_return_value}"

        sub_step_two_return_value = prepare_overlay_expression(
            cel_env=cel_env,
            spec={"value": "17171"},
            location="unittest:sub_step_two.return",
        )

        assert is_unwrapped_ok(
            sub_step_two_return_value
        ), f"{sub_step_two_return_value}"

        sub_workflow = workflow_structure.Workflow(
            name="unit-test",
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.dev", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            steps=[
                workflow_structure.Step(
                    label="sub_step",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=sub_step_return_value,
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_step': value}")),
                ),
                workflow_structure.Step(
                    label="sub_step_two",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=sub_step_two_return_value,
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_step_two': value}")),
                ),
            ],
            dynamic_input_keys=set(),
        )

        workflow = workflow_structure.Workflow(
            name="unit-test",
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.dev", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            steps=[
                workflow_structure.Step(
                    label="sub_workflow",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=sub_workflow,
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_workflow': value}")),
                )
            ],
            dynamic_input_keys=set(),
        )

        workflow_result = await reconcile.reconcile_workflow(
            api=None,
            workflow_key="test-case",
            owner=("unit-tests", celtypes.MapType({"uid": "sam-123"})),
            trigger=celtypes.MapType({}),
            workflow=workflow,
        )
        print(workflow_result)

        self.maxDiff = None
        self.assertDictEqual(
            {
                "sub_workflow": {
                    "sub_step": {"sub_one": True},
                    "sub_step_two": {"value": 17171},
                }
            },
            workflow_result.state,
        )

        # TODO: Check Condition

    async def test_partial_state(self):
        cel_env = celpy.Environment()

        workflow = workflow_structure.Workflow(
            name="unit-test",
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.dev", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            steps=[
                workflow_structure.Step(
                    label="ok_step_one",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=prepare_overlay_expression(
                            cel_env=cel_env, spec={"i_am_ok": True}, location="unittest"
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'first': value}")),
                ),
                workflow_structure.Step(
                    label="fail_step",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=cel_env.program(cel_env.compile("1 / 0")),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'failed_step': value}")),
                ),
                workflow_structure.Step(
                    label="ok_step_two",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        preconditions=None,
                        local_values=None,
                        return_value=prepare_overlay_expression(
                            cel_env=cel_env, spec={"sub_one": True}, location="unittest"
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(
                        cel_env.compile("{'number_two': 2, 'two_value': value}")
                    ),
                ),
            ],
            dynamic_input_keys=set(),
        )

        workflow_result = await reconcile.reconcile_workflow(
            api=None,
            workflow_key="test-case",
            owner=("unit-tests", celtypes.MapType({"uid": "sam-123"})),
            trigger=celtypes.MapType({}),
            workflow=workflow,
        )

        self.maxDiff = None
        self.assertDictEqual(
            {
                "first": {"i_am_ok": True},
                "number_two": 2,
                "two_value": {"sub_one": True},
            },
            workflow_result.state,
        )

        # TODO: Check Condition

    async def test_ref_switch(self):
        cel_env = celpy.Environment()
        source_return_value = prepare_overlay_expression(
            cel_env=cel_env,
            spec={"value": "static-value"},
            location="unittest",
        )

        step_state = cel_env.program(cel_env.compile("{'single_switch': value}"))

        workflow = workflow_structure.Workflow(
            name="unit-test",
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.dev", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            steps=[
                workflow_structure.Step(
                    label="single_switch",
                    skip_if=None,
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=workflow_structure.LogicSwitch(
                        switch_on=cel_env.program(cel_env.compile("'alpha'")),
                        logic_map={
                            "alpha": function_structure.ValueFunction(
                                preconditions=None,
                                local_values=None,
                                return_value=source_return_value,
                                dynamic_input_keys=set(),
                            ),
                        },
                        default_logic=None,
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=step_state,
                )
            ],
            dynamic_input_keys=set(),
        )

        workflow_result = await reconcile.reconcile_workflow(
            api=None,
            workflow_key="test-case",
            owner=("unit-tests", celtypes.MapType({"uid": "sam-123"})),
            trigger=celtypes.MapType({}),
            workflow=workflow,
        )

        self.maxDiff = None
        print(workflow_result.result)
        self.assertDictEqual(
            {"single_switch": {"value": "static-value"}},
            workflow_result.state,
        )

        # TODO: Check Condition


class TestConditionHelper(unittest.TestCase):
    def test_none_outcome(self):
        condition = reconcile._condition_helper(
            condition_type="UnitTest",
            thing_name="unit test",
            outcome=None,
            workflow_key="unit-test-flow",
        )

        self.assertEqual("Pending", condition.get("reason"))

    def test_ok_outcome_that_is_none(self):
        condition = reconcile._condition_helper(
            condition_type="UnitTest",
            thing_name="unit test",
            outcome=Ok(None),
            workflow_key="unit-test-flow",
        )

        self.assertEqual("Ready", condition.get("reason"))

    def test_falsy_outcomes(self):
        falsy_stuff = [[], (), {}, 0, False, ""]

        for falsy_thing in falsy_stuff:
            condition = reconcile._condition_helper(
                condition_type="UnitTest",
                thing_name="unit test",
                outcome=falsy_thing,
                workflow_key="unit-test-flow",
            )

            self.assertEqual(
                "Ready", condition.get("reason"), f"Falsy thing '{falsy_thing}'"
            )
