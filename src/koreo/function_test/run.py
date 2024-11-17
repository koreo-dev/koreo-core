from contextlib import asynccontextmanager

import json

from celpy import celtypes

from koreo.result import UnwrappedOutcome

from koreo.function.reconcile import reconcile_function, _convert_bools

from .structure import (
    Function,
    FunctionTest,
)


class MockResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class MockApi:
    def __init__(self, *args, **kwargs):
        self._materialized = {}

    @property
    def namespace(self):
        return "FAKE-NAMESPACE"

    async def async_get(self, *args, **kwargs):
        return []

    @asynccontextmanager
    async def call_api(self, *args, **kwargs):
        # raise Exception("called")
        data = json.loads(kwargs.get("data", "{}"))
        self._materialized = data
        yield MockResponse(data=data)


async def run_function_test(
    location: str, function_test: FunctionTest
) -> UnwrappedOutcome:
    api = MockApi()
    result = await reconcile_function(
        api=api,
        location=f"{location}:",
        function=function_test.function_under_test,
        trigger=function_test.parent,
        inputs=function_test.inputs,
    )

    json_expected = json.loads(
        json.dumps(_convert_bools(function_test.expected_resource))
    )

    return (api._materialized, json_expected)
