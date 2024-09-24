import logging


import kopf
import kr8s

from koreo.result import combine, is_error, raise_for_error

from koreo.cache import get_resource_from_cache
from koreo.workflow.reconcile import reconcile_workflow
from koreo.workflow.registry import get_custom_crd_workflows
from koreo.workflow.structure import Workflow

from koreo.workflow import registry


@kopf.on.login()
async def login_fn(logger, *_, **__):
    kr8s_api = kr8s.api()

    await kr8s_api.auth.reauthenticate()

    logger.info(
        "*****LOGIN LOGIN LOGIN Party down like a clown  LOGIN LOGIN LOGIN*****"
    )
    print("*****LOGIN LOGIN LOGIN Party down like a clown LOGIN LOGIN LOGIN*****")

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
        meta: kopf.Meta, spec: kopf.Spec, patch: kopf.Patch, *_, **__
    ):
        logging.info(f"Reconciling {key}")

        kr8s_api = kr8s.api()

        workflow_keys = get_custom_crd_workflows(custom_crd=key)

        trigger = {"metadata": dict(meta), "spec": dict(spec)}

        outcomes = {}
        for workflow_key in workflow_keys:
            workflow = get_resource_from_cache(
                resource_type=Workflow, cache_key=workflow_key
            )
            if not workflow:
                logging.error("Missing Workflow!")
                return
            logging.info(f"Running Workflow {workflow_key}")
            outcomes[workflow_key] = await reconcile_workflow(
                api=kr8s_api,
                workflow_key=workflow_key,
                trigger=trigger,
                workflow=workflow,
            )
            logging.info(f"Workflow {workflow_key} outcomes: {outcomes}")

        if outcomes:
            error_outcomes = [
                outcome for outcome in outcomes.values() if is_error(outcome)
            ]
            error_outcome = combine(error_outcomes)
            if is_error(error_outcome):
                patch.update({"status": {"koreo": error_outcome.message}})
                raise_for_error(error_outcome)

            patch.update({"status": {"koreo": outcomes}})

        return True
