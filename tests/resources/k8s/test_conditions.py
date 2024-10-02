from datetime import UTC, datetime
from unittest.mock import patch
import unittest

from resources.k8s.conditions import Condition, Conditions, update_condition


class TestConditions(unittest.TestCase):
    def test_new(self):
        conditions: Conditions = []
        update: Condition = Condition(
            type="NewTest",
            reason="Testing",
            status="AwaitDependency",
            message="Testing new condition insertion.",
            location="testing.location",
        )

        frozen_now = datetime.now(UTC)
        with patch("resources.k8s.conditions.datetime") as frozen_now_datetime:
            frozen_now_datetime.now.return_value = frozen_now
            updated_conditions = update_condition(
                conditions=conditions, condition=update
            )

        self.assertFalse(conditions)
        self.assertEqual(1, len(updated_conditions))

        condition = updated_conditions[0]
        self.assertDictEqual(
            condition,
            {
                "lastUpdateTime": frozen_now.isoformat(),
                "lastTransitionTime": frozen_now.isoformat(),
                "type": "NewTest",
                "reason": "Testing",
                "status": "AwaitDependency",
                "message": "Testing new condition insertion.",
                "location": "testing.location",
            },
        )

    def test_non_update_update(self):
        conditions: Conditions = []
        condition: Condition = Condition(
            type="UpdateTest",
            reason="Done",
            status="Ready",
            message="All set.",
            location="testing.location",
        )

        insert_frozen_now = datetime.now(UTC)
        with patch("resources.k8s.conditions.datetime") as frozen_now_datetime:
            frozen_now_datetime.now.return_value = insert_frozen_now

            updated_conditions = update_condition(
                conditions=conditions, condition=condition
            )

        update_frozen_now = datetime.now(UTC)
        with patch("resources.k8s.conditions.datetime") as frozen_now_datetime:
            frozen_now_datetime.now.return_value = update_frozen_now

            updated_conditions = update_condition(
                conditions=updated_conditions, condition=condition
            )

        self.assertFalse(conditions)
        self.assertEqual(1, len(updated_conditions))

        self.assertDictEqual(
            updated_conditions[0],
            {
                "lastUpdateTime": update_frozen_now.isoformat(),
                "lastTransitionTime": insert_frozen_now.isoformat(),
                "type": "UpdateTest",
                "reason": "Done",
                "status": "Ready",
                "message": "All set.",
                "location": "testing.location",
            },
        )

    def test_multiple_conditions(self):
        conditions: Conditions = []
        update_one: Condition = Condition(
            type="UpdateTest",
            reason="Testing",
            status="AwaitDependency",
            message="Testing new condition insertion.",
            location="testing.location.1",
        )

        updated_conditions = update_condition(
            conditions=conditions, condition=update_one
        )

        new: Condition = Condition(
            type="NewTest",
            reason="Waiting",
            status="AwaitDependency",
            message="Testing new condition insertion.",
            location="testing.location.new",
        )

        new_frozen_now = datetime.now(UTC)
        with patch("resources.k8s.conditions.datetime") as frozen_now_datetime:
            frozen_now_datetime.now.return_value = new_frozen_now

            updated_conditions = update_condition(
                conditions=updated_conditions, condition=new
            )

        update_two: Condition = Condition(
            type="UpdateTest",
            reason="Done",
            status="Ready",
            message="All set.",
            location="testing.location.2",
        )

        update_frozen_now = datetime.now(UTC)
        with patch("resources.k8s.conditions.datetime") as frozen_now_datetime:
            frozen_now_datetime.now.return_value = update_frozen_now

            updated_conditions = update_condition(
                conditions=updated_conditions, condition=update_two
            )

        self.assertFalse(conditions)
        self.assertEqual(2, len(updated_conditions))

        # NOTE: I do not like how this assumes a specific order of conditions
        self.assertDictEqual(
            updated_conditions[0],
            {
                "lastUpdateTime": update_frozen_now.isoformat(),
                "lastTransitionTime": update_frozen_now.isoformat(),
                "type": "UpdateTest",
                "reason": "Done",
                "status": "Ready",
                "message": "All set.",
                "location": "testing.location.2",
            },
        )

        self.assertDictEqual(
            updated_conditions[1],
            {
                "lastUpdateTime": new_frozen_now.isoformat(),
                "lastTransitionTime": new_frozen_now.isoformat(),
                "type": "NewTest",
                "reason": "Waiting",
                "status": "AwaitDependency",
                "message": "Testing new condition insertion.",
                "location": "testing.location.new",
            },
        )
