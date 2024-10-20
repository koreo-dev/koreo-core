import os

import celpy
import requests

if os.environ.get("PYTEST_VERSION") is not None:
    PROJECT_NUMBER = "1234567890"
    PROJECT_ID = "unittest-project-id"
elif os.environ.get("KOREO_DEV_TOOLING", False):
    PROJECT_NUMBER = "1234567890"
    PROJECT_ID = "dev-tooling-project-id"
else:
    PROJECT_NUMBER = os.environ.get("PROJECT_NUMBER")
    if not PROJECT_NUMBER:
        PROJECT_NUMBER = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/numeric-project-id",
            headers={"Metadata-Flavor": "Google"},
        ).text

    PROJECT_ID = os.environ.get("PROJECT_ID")
    if not PROJECT_ID:
        PROJECT_ID = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
        ).text

env = celpy.json_to_cel(
    {
        "project": {
            "number": PROJECT_NUMBER,
            "id": PROJECT_ID,
            "selfRef": {
                "apiVersion": "resourcemanager.cnrm.cloud.google.com/v1beta1",
                "kind": "Project",
                "external": f"projects/{PROJECT_ID}",
            },
        }
    }
)
