"""Continuously flush ingestion-edge queue from detached persistent volumes."""

from argparse import ArgumentParser
from functools import partial
from multiprocessing.pool import ThreadPool
from time import sleep
from traceback import print_exc
import json
import os

from kubernetes.config import load_kube_config
from kubernetes.client import CoreV1Api, BatchV1Api, V1DeleteOptions, V1Preconditions
from kubernetes.client.rest import ApiException

from .config import get_config_dict

CONFLICT = "Conflict"
NOT_FOUND = "Not Found"
ALREADY_EXISTS = "AlreadyExists"

DEFAULT_IMAGE_VERSION = "latest"
try:
    with open("version.json") as fp:
        DEFAULT_IMAGE_VERSION = str(
            json.load(fp).get("version") or DEFAULT_IMAGE_VERSION
        )
except (FileNotFoundError, json.decoder.JSONDecodeError):
    pass  # use default

DEFAULT_ENVIRONMENT = {
    key: os.environ[key] for key in get_config_dict() if key in os.environ
}

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--command",
    default=["python", "-m", "ingestion_edge.flush"],
    type=json.loads,
    help="Docker command for flush jobs; must drain queue until empty or exit non-zero",
)
parser.add_argument(
    "--environment",
    default=DEFAULT_ENVIRONMENT,
    type=json.loads,
    help="Environment to use for flush jobs",
)
parser.add_argument(
    "--image",
    default="mozilla/ingestion-edge:" + DEFAULT_IMAGE_VERSION,
    help="Docker image to use for flush jobs",
)
parser.add_argument(
    "--claim-prefix",
    default="queue-",
    help="Prefix for the names of persistent volume claims to delete",
)


def _create_pvc(api, name, namespace, pv):
    # TODO verify this
    try:
        return api.create_namespaced_persistent_volume_claim(
            namespace,
            body={
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": name, "namespace": namespace},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": pv.spec.capacity.storage}},
                    "storageClassName": pv.spec.storage_class_name,
                    "volumeName": pv.metadata.name,
                },
            },
        )
    except ApiException as e:
        if e.reason == ALREADY_EXISTS:
            return api.read_namespaced_persistent_volume_claim(name, namespace)
        raise


def _bind_pvc(api, pv, pvc):
    # TODO verify this is idempotent
    api.patch_persistent_volume(
        pv.metadata.name,
        body={
            "spec": {
                "claimRef": {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "name": pvc.metadata.name,
                    "namespace": pvc.metadata.namespace,
                    "resourceVersion": pvc.metadata.resource_version,
                    "uid": pvc.metadata.uid,
                },
                "persistentVolumeReclaimPolicy": "Delete",
            }
        },
    )


def _create_flush_job(batch_api, command, environment, image, name, namespace):
    # TODO verify this
    try:
        return batch_api.create_namespaced_job(
            namespace,
            body={
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": name, "namespace": namespace},
                "spec": {
                    "containers": [
                        {
                            "image": image,
                            "command": command,
                            "name": "flush",
                            "volumeMounts": [{"mountPath": "/data", "name": "queue"}],
                            "environment": environment,
                        }
                    ],
                    "restartPolicy": "OnFailure",
                    "volumes": [
                        {"name": "queue", "persistentVolumeClaim": {"claimName": name}}
                    ],
                },
            },
        )
    except ApiException as e:
        if e.reason == ALREADY_EXISTS:
            return batch_api.read_namespaced_job(name, namespace)
        raise


def flush_released_pvs(api, batch_api, command, environment, image, namespace):
    """
    Flush persistent volumes.

    Gracefully handle resuming after an interruption, because this is not atomic.
    """
    for pv in api.list_persistent_volume():
        name = "flush-" + pv.metadata.name
        if (
            pv.spec.claim_ref
            and pv.spec.claim_ref.namespace == namespace
            and pv.status
            and (pv.status.phase == "Released" or pv.spec.claim_ref.name == name)
        ):
            if pv.status.phase != "Bound":
                pvc = _create_pvc(api, name, namespace, pv)
                _bind_pvc(api, pv, pvc)
            _create_flush_job(batch_api, command, environment, image, name, namespace)


def delete_complete_jobs(api, batch_api, namespace):
    """Delete complete jobs."""
    for job in batch_api.list_namespaced_job(namespace):
        if (
            job.status.conditions
            and job.status.conditions[0].type == "Complete"
            and not job.metadata.deletion_timestamp
        ):
            # configure persistent volume claims to be deleted with the job
            for volume in job.spec.template.spec.volumes:
                if (
                    volume.persistent_volume_claim
                    and volume.persistent_volume_claim.claim_name
                ):
                    api.patch_namespaced_persistent_volume_claim(
                        volume.persistent_volume_claim.claim_name,
                        namespace,
                        {
                            "metadata": {
                                "ownerReferences": [
                                    {
                                        "apiVersion": job.api_version,
                                        "kind": job.kind,
                                        "name": job.metadata.name,
                                        "uid": job.metadata.uid,
                                        "blockOwnerDeletion": True,
                                    }
                                ]
                            }
                        },
                    )
            try:
                batch_api.delete_namespaced_job(
                    job.metadata.name,
                    namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Foreground",
                        preconditions=V1Preconditions(
                            resource_version=job.metadata.resource_version,
                            uid=job.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason in (CONFLICT, NOT_FOUND):
                    continue  # already deleted or replaced
                raise


def _unschedulable_due_to_pvc(pod):
    return (
        pod.status
        and pod.status.phase == "Pending"
        and (condition := (pod.status.conditions or [None])[0])
        and condition.reason == "Unschedulable"
        and condition.message
        and condition.message.startswith('persistentvolumeclaim "')
        and condition.message.endswith('" not found')
        and pod.metadata.owner_references
        and any(ref.kind == "StatefulSet" for ref in pod.metadata.owner_references)
    )


def delete_detached_pvcs(api, namespace, claim_prefix):
    """
    Delete persistent volume claims that are not attached to any pods.

    If a persistent volume claim is deleted while attached to a pod, then the
    underlying persistent volume will remain bound until the delete is
    finalized, and the delete will not be finalized until the pod is also
    deleted.

    If a stateful set immediately recreates a pod (e.g. via `kubectl rollout
    restart`) that was attached to a persistent volume claim that was deleted,
    then the stateful set may still try to reuse the persistent volume claim
    after the delete is finalized. Delete the pod again to cause the stateful
    set to recreate the persistent volume claim when it next recreates the pod.
    """
    attached_pvcs = {
        volume.persistent_volume_claim.claim_name
        for pod in api.list_namespaced_pod(namespace)
        if not _unschedulable_due_to_pvc(pod) and pod.spec and pod.spec.volumes
        for volume in pod.spec.volumes
        if volume.persistent_volume_claim and volume.persistent_volume_claim.claim_name
    }
    for pvc in api.list_namespaced_persistent_volume_claim(namespace):
        if (
            pvc.metadata.name.startswith(claim_prefix)
            and pvc.metadata.name not in attached_pvcs
            and not pvc.metadata.deletion_timestamp
        ):
            try:
                api.delete_namespaced_persistent_volume_claim(
                    pvc.metadata.name,
                    namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Background",
                        preconditions=V1Preconditions(
                            resource_version=pvc.metadata.resource_version,
                            uid=pvc.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason in (CONFLICT, NOT_FOUND):
                    continue  # already deleted or replaced
                raise


def delete_unschedulable_pods(api, namespace):
    """
    Delete pods that are unschedulable due to a missing persistent volume claim.

    A stateful set may create a pod attached to a missing persistent volume
    claim if the pod is recreated while the persistent volume claim is pending
    delete.

    When this happens, delete the pod so that the stateful set will create a
    new persistent volume claim when it next creates the pod.
    """
    for pod in api.list_namespaced_pod(namespace):
        if _unschedulable_due_to_pvc(pod):
            try:
                api.delete_namespaced_pod(
                    pod.metadata.name,
                    namespace,
                    body=V1DeleteOptions(
                        grace_period_seconds=0,
                        propagation_policy="Background",
                        preconditions=V1Preconditions(
                            resource_version=pod.metadata.resource_version,
                            uid=pod.metadata.uid,
                        ),
                    ),
                )
            except ApiException as e:
                if e.reason in (CONFLICT, NOT_FOUND):
                    continue  # already deleted or replaced
                raise


def run_task(func):
    """Continuously run func and print exceptions."""
    while True:
        try:
            func()
        except Exception:
            print_exc()
        else:
            sleep(1)


def main():
    """Continuously flush and delete detached persistent volumes."""
    args = parser.parse_args()
    load_kube_config()
    api = CoreV1Api()
    batch_api = BatchV1Api()
    tasks = [
        partial(
            flush_released_pvs,
            api,
            batch_api,
            args.command,
            args.environment,
            args.image,
            args.namespace,
        ),
        partial(delete_complete_jobs, api, batch_api, args.namespace),
        partial(delete_detached_pvcs, api, args.namespace, args.claim_prefix),
        partial(delete_unschedulable_pods, api, args.namespace),
    ]
    with ThreadPool(len(tasks)) as pool:
        pool.map(run_task, tasks, chunksize=1)


if __name__ == "__main__":
    main()
