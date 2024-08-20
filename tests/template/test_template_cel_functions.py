import datetime
import random
import string
import unittest

import celpy


from koreo.template.template_cel_functions import functions, function_annotations


def _random_string():
    return "".join(random.choices(string.ascii_letters, k=random.randint(5, 50)))


class SelfRefTests(unittest.TestCase):
    def test_bad_type(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate({"resource": celpy.json_to_cel("test")})

    def test_missing_api_version(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        namespace = _random_string()
        kind = _random_string()

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(
                {
                    "resource": celpy.json_to_cel(
                        {
                            "kind": kind,
                            "metadata": {
                                "name": name,
                                "namespace": namespace,
                                "junk": _random_string(),
                            },
                            "junk": _random_string(),
                        }
                    )
                }
            )

    def test_missing_kind(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        namespace = _random_string()
        api_version = _random_string()

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(
                {
                    "resource": celpy.json_to_cel(
                        {
                            "apiVersion": api_version,
                            "metadata": {
                                "name": name,
                                "namespace": namespace,
                            },
                        }
                    )
                }
            )

    def test_missing_metadata(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        api_version = _random_string()
        kind = _random_string()

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(
                {
                    "resource": celpy.json_to_cel(
                        {
                            "apiVersion": api_version,
                            "kind": kind,
                        }
                    )
                }
            )

    def test_missing_name(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        namespace = _random_string()
        api_version = _random_string()
        kind = _random_string()

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(
                {
                    "resource": celpy.json_to_cel(
                        {
                            "apiVersion": api_version,
                            "kind": kind,
                            "metadata": {
                                "namespace": namespace,
                            },
                        }
                    )
                }
            )

    def test_missing_namespace(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        api_version = _random_string()
        kind = _random_string()

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate(
                {
                    "resource": celpy.json_to_cel(
                        {
                            "apiVersion": api_version,
                            "kind": kind,
                            "metadata": {
                                "name": name,
                            },
                        }
                    )
                }
            )

    def test_ok(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.self_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        namespace = _random_string()
        api_version = _random_string()
        kind = _random_string()

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "apiVersion": api_version,
                        "kind": kind,
                        "metadata": {
                            "name": name,
                            "namespace": namespace,
                            "junk": _random_string(),
                        },
                        "junk": _random_string(),
                    }
                )
            }
        )

        self.maxDiff = None
        self.assertEqual(
            value,
            {
                "name": name,
                "namespace": namespace,
                "apiVersion": api_version,
                "kind": kind,
            },
        )


class ToRefTests(unittest.TestCase):
    def test_bad_type(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        with self.assertRaises(celpy.CELEvalError):
            program.evaluate({"input": celpy.json_to_cel(7)})

    def test_empty_map(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        with self.assertRaisesRegex(celpy.CELEvalError, "required"):
            program.evaluate({"input": celpy.json_to_cel({})})

    def test_empty_external(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        with self.assertRaisesRegex(celpy.CELEvalError, "must contain"):
            program.evaluate({"input": celpy.json_to_cel({"external": ""})})

    def test_external(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        external = _random_string()
        value = program.evaluate({"input": celpy.json_to_cel({"external": external})})

        self.assertEqual(value, {"external": external})

    def test_external_full(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        external = _random_string()
        api_version = _random_string()
        kind = _random_string()

        value = program.evaluate(
            {
                "input": celpy.json_to_cel(
                    {
                        "external": external,
                        "apiVersion": api_version,
                        "kind": kind,
                    }
                )
            }
        )

        self.assertEqual(
            value,
            {
                "external": external,
                "apiVersion": api_version,
                "kind": kind,
            },
        )

    def test_empty_name(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        with self.assertRaisesRegex(celpy.CELEvalError, "must contain"):
            program.evaluate({"input": celpy.json_to_cel({"name": ""})})

    def test_name(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        value = program.evaluate({"input": celpy.json_to_cel({"name": name})})

        self.assertEqual(value, {"name": name})

    def test_full(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("input.to_ref()")

        program = env.program(compiled, functions=functions)

        name = _random_string()
        namespace = _random_string()
        api_version = _random_string()
        kind = _random_string()

        value = program.evaluate(
            {
                "input": celpy.json_to_cel(
                    {
                        "name": name,
                        "namespace": namespace,
                        "apiVersion": api_version,
                        "kind": kind,
                        "junk": _random_string(),
                    }
                )
            }
        )

        self.maxDiff = None
        self.assertEqual(
            value,
            {
                "name": name,
                "namespace": namespace,
                "apiVersion": api_version,
                "kind": kind,
            },
        )


class ConfigConnectReadyTests(unittest.TestCase):
    def test_bad_type(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        value = program.evaluate({"resource": celpy.json_to_cel("a")})
        self.assertEqual(value, False)

    def test_no_status(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_no_conditions(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_no_ready_conditions(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            "conditions": [
                                {
                                    "lastTransitionTime": f"{datetime.datetime.now()}",
                                    "message": _random_string(),
                                    "reason": _random_string(),
                                    "status": _random_string(),
                                    "type": _random_string(),
                                }
                                for _ in range(random.randint(1, 10))
                            ],
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_non_uptodate_ready_condition(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        conditions = [
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": _random_string(),
                "status": _random_string(),
                "type": _random_string(),
            }
            for _ in range(random.randint(1, 10))
        ]
        conditions.append(
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": _random_string(),
                "status": _random_string(),
                "type": "Ready",
            }
        )

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            "conditions": conditions,
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_non_ready_condition(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        conditions = [
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": _random_string(),
                "status": _random_string(),
                "type": _random_string(),
            }
            for _ in range(random.randint(1, 10))
        ]
        conditions.append(
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": "UpToDate",
                "status": _random_string(),
                "type": "Ready",
            }
        )

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            "conditions": conditions,
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_multiple_ready_conditions(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        conditions = [
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": _random_string(),
                "status": _random_string(),
                "type": _random_string(),
            }
            for _ in range(random.randint(1, 10))
        ]
        conditions.append(
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": "UpToDate",
                "status": "True",
                "type": "Ready",
            }
        )

        conditions.append(
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": "UpToDate",
                "status": "True",
                "type": "Ready",
            }
        )

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            "conditions": conditions,
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, False)

    def test_ready_condition(self):
        env = celpy.Environment(annotations=function_annotations)

        compiled = env.compile("resource.config_connect_ready()")

        program = env.program(compiled, functions=functions)

        conditions = [
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": _random_string(),
                "status": _random_string(),
                "type": _random_string(),
            }
            for _ in range(random.randint(1, 10))
        ]
        conditions.append(
            {
                "lastTransitionTime": f"{datetime.datetime.now()}",
                "message": _random_string(),
                "reason": "UpToDate",
                "status": "True",
                "type": "Ready",
            }
        )

        value = program.evaluate(
            {
                "resource": celpy.json_to_cel(
                    {
                        "kind": _random_string(),
                        "metadata": {
                            "name": _random_string(),
                            "namespace": _random_string(),
                            "junk": _random_string(),
                        },
                        "spec": {
                            "junk": _random_string(),
                        },
                        "status": {
                            "junk": _random_string(),
                            "conditions": conditions,
                            _random_string(): _random_string(),
                        },
                    }
                )
            }
        )
        self.assertEqual(value, True)
