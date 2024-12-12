import unittest

import celpy
from celpy import celtypes

from koreo.result import Ok

from koreo.cel.structure_extractor import extract_argument_structure

from koreo.function import structure as function_structure

from koreo.workflow import reconcile
from koreo.workflow import structure as workflow_structure


class TestReconcileWorkflow(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile(self):
        cel_env = celpy.Environment()
        source_ok_value = cel_env.program(
            cel_env.compile("{'resources': [{'bool': true}, {'bool': false}]}")
        )
        used_vars = set(extract_argument_structure(source_ok_value.ast))

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            status=workflow_structure.Status(conditions=[], state=None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="input_source",
                    mapped_input=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.Function(
                        resource_config=function_structure.StaticResource(
                            behavior=function_structure.Behavior(
                                load="virtual",
                                create=False,
                                update="never",
                                delete="abandon",
                            ),
                            managed_resource=None,
                            context=celtypes.MapType({}),
                        ),
                        input_validators=None,
                        outcome=function_structure.Outcome(
                            validators=None, ok_value=source_ok_value
                        ),
                        materializers=function_structure.Materializers(
                            base=None, on_create=None
                        ),
                        dynamic_input_keys=used_vars,
                    ),
                    condition=None,
                )
            ],
        )

        value, conditions = await reconcile.reconcile_workflow(
            api=None, workflow_key="test-case", trigger={}, workflow=workflow
        )

        self.maxDiff = None
        self.assertDictEqual(
            {"input_source": {"resources": [{"bool": True}, {"bool": False}]}}, value
        )

        # TODO: Check Condition

    async def test_reconcile_nested(self):
        cel_env = celpy.Environment()
        source_ok_value = cel_env.program(
            cel_env.compile("{'resources': [{'bool': true}, {'bool': false}]}")
        )
        used_vars = set(extract_argument_structure(source_ok_value.ast))

        sub_workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            status=workflow_structure.Status(conditions=[], state=None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="sub_step",
                    mapped_input=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.Function(
                        resource_config=function_structure.StaticResource(
                            behavior=function_structure.Behavior(
                                load="virtual",
                                create=False,
                                update="never",
                                delete="abandon",
                            ),
                            managed_resource=None,
                            context=celtypes.MapType({}),
                        ),
                        input_validators=None,
                        outcome=function_structure.Outcome(
                            validators=None,
                            ok_value=cel_env.program(
                                cel_env.compile("{'sub_one': true}")
                            ),
                        ),
                        materializers=function_structure.Materializers(
                            base=None, on_create=None
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                ),
                workflow_structure.Step(
                    label="sub_step_two",
                    mapped_input=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=function_structure.Function(
                        resource_config=function_structure.StaticResource(
                            behavior=function_structure.Behavior(
                                load="virtual",
                                create=False,
                                update="never",
                                delete="abandon",
                            ),
                            managed_resource=None,
                            context=celtypes.MapType({}),
                        ),
                        input_validators=None,
                        outcome=function_structure.Outcome(
                            validators=None,
                            ok_value=cel_env.program(cel_env.compile("17171")),
                        ),
                        materializers=function_structure.Materializers(
                            base=None, on_create=None
                        ),
                        dynamic_input_keys=set(),
                    ),
                    condition=None,
                ),
            ],
        )

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            status=workflow_structure.Status(conditions=[], state=None),
            config_step=None,
            steps=[
                workflow_structure.Step(
                    label="sub_workflow",
                    mapped_input=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    logic=sub_workflow,
                    condition=None,
                )
            ],
        )

        value, conditions = await reconcile.reconcile_workflow(
            api=None, workflow_key="test-case", trigger={}, workflow=workflow
        )

        self.maxDiff = None
        self.assertDictEqual(
            {"sub_workflow": {"sub_step": {"sub_one": True}, "sub_step_two": 17171}},
            value,
        )

        # TODO: Check Condition
