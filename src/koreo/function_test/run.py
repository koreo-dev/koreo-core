from contextlib import asynccontextmanager
from typing import NamedTuple
import json

import kr8s

from celpy import celtypes

from koreo.result import Outcome, UnwrappedOutcome, is_unwrapped_ok

from koreo.function.reconcile import reconcile_function, _convert_bools

from .structure import FunctionTest


class MockResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class MockApi:
    def __init__(self, current_resource: dict | None, *args, **kwargs):
        self._current_resource = current_resource
        self._materialized = None

    @property
    def materialized(self):
        return self._materialized

    @property
    def namespace(self):
        return "FAKE-NAMESPACE"

    async def async_get(self, *args, **kwargs):
        if self._current_resource:
            # TODO: This should probably be loaded and built from the Function,
            # using _build_resource_config

            resource_class = kr8s.objects.new_class(
                version=self._current_resource.get("apiVersion"),
                kind=self._current_resource.get("kind"),
                namespaced=(
                    True
                    if self._current_resource.get("metadata", {}).get("namespace")
                    else False
                ),
                asyncio=True,
            )
            return [
                resource_class(
                    api=self,
                    resource=self._current_resource,
                    namespace=self._current_resource.get("metadata", {}).get(
                        "namespace"
                    ),
                )
            ]
        else:
            return []

    @asynccontextmanager
    async def call_api(self, *args, **kwargs):
        # raise Exception("called")
        data = json.loads(kwargs.get("data", "{}"))
        self._materialized = data
        yield MockResponse(data=data)


class TestResults(NamedTuple):
    expected_resource: dict | None
    expected_outcome: Outcome | None
    expected_return: dict | None

    actual_resource: dict | None
    outcome: UnwrappedOutcome[dict]


async def run_function_test(location: str, function_test: FunctionTest) -> TestResults:
    api = MockApi(current_resource=function_test.current_resource)

    result = await reconcile_function(
        api=api,
        location=location,
        function=function_test.function_under_test,
        trigger=celtypes.MapType({}),
        inputs=function_test.inputs,
    )

    if is_unwrapped_ok(result):
        result = json.loads(json.dumps(_convert_bools(result)))

    return TestResults(
        expected_resource=function_test.expected_resource,
        expected_return=function_test.expected_return,
        expected_outcome=function_test.expected_outcome,
        actual_resource=api.materialized,
        outcome=result,
    )
