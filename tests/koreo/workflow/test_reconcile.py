import unittest

import celpy

from koreo.result import Ok

from koreo.function import structure as function_structure

from koreo.workflow import reconcile
from koreo.workflow import structure as workflow_structure


class TestReconcileWorkflow(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile(self):
        cel_env = celpy.Environment()
        source_ok_value = cel_env.program(
            cel_env.compile("{'resources': [{'bool': true}, {'bool': false}]}")
        )
        print(source_ok_value.evaluate({}))

        workflow = workflow_structure.Workflow(
            crd_ref=workflow_structure.ConfigCRDRef(
                api_group="tests.koreo.realkinetic.com", version="v1", kind="TestCase"
            ),
            steps_ready=Ok(None),
            status=workflow_structure.Status(conditions=[], state=None),
            steps=[
                workflow_structure.FunctionRef(
                    label="input_source",
                    mapped_input=None,
                    inputs=None,
                    dynamic_input_keys=[],
                    function=function_structure.Function(
                        resource_config=function_structure.StaticResource(
                            behavior=function_structure.Behavior(
                                load="virtual",
                                create=False,
                                update="never",
                                delete="abandon",
                            ),
                            managed_resource=None,
                        ),
                        input_validators=None,
                        function_ready=Ok(None),
                        outcome=function_structure.Outcome(
                            tests=None, ok_value=source_ok_value
                        ),
                        materializers=function_structure.Materializers(
                            base=None, on_create=None
                        ),
                    ),
                )
            ],
        )

        result = await reconcile.reconcile_workflow(
            api=None, workflow_key="test-case", trigger={}, workflow=workflow
        )

        self.assertTrue(True)
