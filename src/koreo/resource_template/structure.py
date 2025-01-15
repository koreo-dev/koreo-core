from typing import NamedTuple

from celpy import celtypes


class ResourceTemplate(NamedTuple):
    context: celtypes.MapType

    template: celtypes.MapType
