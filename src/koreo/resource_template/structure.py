from typing import NamedTuple

from koreo.result import Outcome

from koreo.function.structure import ManagedResource, Behavior


class ResourceTemplate(NamedTuple):
    template_name: str

    managed_resource: ManagedResource
    behavior: Behavior

    context: dict

    template: dict

    valid: Outcome
