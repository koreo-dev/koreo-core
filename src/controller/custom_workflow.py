import logging


import celpy
import kopf
import kr8s

from resources.k8s.conditions import Condition, update_condition

from koreo.result import is_error, raise_for_error

from koreo.cache import get_resource_from_cache
from koreo.cel.encoder import convert_bools
from koreo.workflow.reconcile import reconcile_workflow
from koreo.workflow.registry import get_custom_crd_workflows
from koreo.workflow.structure import Workflow


@kopf.on.login()
async def login_fn(logger, *_, **__):
    kr8s_api = kr8s.api()

    await kr8s_api.auth.reauthenticate()

    logger.info("Reauthenticating kopf.")

    return kopf.ConnectionInfo(
        server=kr8s_api.auth.server,
        ca_path=(
            f"{kr8s_api.auth.server_ca_file}" if kr8s_api.auth.server_ca_file else None
        ),
        insecure=kr8s_api.auth._insecure_skip_tls_verify,
        username=None,
        password=None,
        token=kr8s_api.auth.token,
        certificate_path=(
            f"{kr8s_api.auth.client_cert_file}"
            if kr8s_api.auth.client_cert_file
            else None
        ),
        private_key_path=(
            f"{kr8s_api.auth.client_key_file}"
            if kr8s_api.auth.client_key_file
            else None
        ),
        default_namespace=kr8s_api.namespace,
        priority=20,
    )


__active_controllers: set[str] = set()


def start_controller(group: str, kind: str, version: str):
    key = f"{group}:{kind}:{version}"
    if key in __active_controllers:
        logging.info(f"Already reconciling {key}")
        return

    logging.info(f"Watching {key}")
    __active_controllers.add(key)

    @kopf.on.create(group=group, kind=kind, version=version)
    @kopf.on.update(group=group, kind=kind, version=version)
    @kopf.on.resume(group=group, kind=kind, version=version)
    async def reconcile_custom_workflow(
        meta: kopf.Meta,
        spec: kopf.Spec,
        status: kopf.Status,
        patch: kopf.Patch,
        *_,
        **__,
    ):
        logging.info(f"Reconciling {key}")

        kr8s_api = kr8s.api()

        workflow_keys = get_custom_crd_workflows(custom_crd=key)

        owner_ref = {
            "apiVersion": f"{group}/{version}",
            "kind": kind,
            "blockOwnerDeletion": True,
            "controller": True,
            "name": meta.name,
            "uid": meta.uid,
        }

        conditions: list[Condition] = status.get("conditions", [])

        if len(workflow_keys) > 1:
            message = f"Multiple Workflows attempted to run ({','.join(workflow_keys)})"
            condition = Condition(
                type="Ready",
                reason="MultipleWorkflows",
                message=message,
                status="false",
                location=f"{';'.join(workflow_keys)}",
            )
            conditions = update_condition(conditions=conditions, condition=condition)

            patch.update(
                {
                    "status": {
                        "conditions": conditions,
                        "koreo": {
                            "errors": message,
                            "locations": f"{';'.join(workflow_keys)}",
                        },
                    }
                }
            )
            raise kopf.TemporaryError(message, delay=120)

        trigger = celpy.json_to_cel(
            {"ownerRef": owner_ref, "metadata": dict(meta), "spec": dict(spec)}
        )

        outcome = None
        for workflow_key in workflow_keys:
            workflow = get_resource_from_cache(
                resource_class=Workflow, cache_key=workflow_key
            )
            if not workflow:
                logging.error("Missing Workflow!")
                return
            logging.info(f"Running Workflow {workflow_key}")

            workflow_outcomes, workflow_conditions = await reconcile_workflow(
                api=kr8s_api,
                workflow_key=workflow_key,
                trigger=trigger,
                workflow=workflow,
            )
            outcome = workflow_outcomes
            for condition in workflow_conditions:
                conditions = update_condition(
                    conditions=conditions, condition=condition
                )

        if not outcome:
            return True

        if is_error(outcome):
            patch.update(
                {
                    "status": {
                        "conditions": conditions,
                        "koreo": {
                            "errors": outcome.message,
                            "locations": outcome.location,
                        },
                    }
                }
            )
            raise_for_error(outcome)

        koreo_value = {
            "errors": None,
            "locations": None,
        }

        patch.update(
            {
                "status": {
                    "conditions": conditions,
                    "koreo": koreo_value,
                    "state": convert_bools(outcome),
                }
            }
        )
