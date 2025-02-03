import unittest

import celpy
from celpy import celtypes

from koreo.result import Ok

from koreo.cel.structure_extractor import extract_argument_structure

from koreo.value_function import structure as function_structure

from koreo.workflow import reconcile
from koreo.workflow import structure as workflow_structure


class TestReconcileWorkflow(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile(self):
        cel_env = celpy.Environment()
        source_return_value = cel_env.program(
            cel_env.compile("{'resources': [{'bool': true}, {'bool': false}]}")
        )

        step_state = cel_env.program(cel_env.compile("{'input_source': value}"))

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="input_source",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=source_return_value,
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=step_state,
                )
            ],
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

        sub_workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="sub_step",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=cel_env.program(
                            cel_env.compile("{'sub_one': true}")
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_step': value}")),
                ),
                workflow_structure.Step(
                    label="sub_step_two",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=cel_env.program(cel_env.compile("17171")),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_step_two': value}")),
                ),
            ],
        )

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="sub_workflow",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=sub_workflow,
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'sub_workflow': value}")),
                )
            ],
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
            {"sub_workflow": {"sub_step": {"sub_one": True}, "sub_step_two": 17171}},
            workflow_result.state,
        )

        # TODO: Check Condition

    async def test_partial_state(self):
        cel_env = celpy.Environment()

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="ok_step_one",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=cel_env.program(
                            cel_env.compile("{'i_am_ok': true}")
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'first': value}")),
                ),
                workflow_structure.Step(
                    label="fail_step",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=cel_env.program(cel_env.compile("1 / 0")),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(cel_env.compile("{'failed_step': value}")),
                ),
                workflow_structure.Step(
                    label="ok_step_two",
                    for_each=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.ValueFunction(
                        validators=None,
                        local_values=None,
                        return_value=cel_env.program(
                            cel_env.compile("{'sub_one': true}")
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                    state=cel_env.program(
                        cel_env.compile("{'number_two': 2, 'two_value': value}")
                    ),
                ),
            ],
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
