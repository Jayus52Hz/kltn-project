import pathlib
import sys
import unittest


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from outcome_script_rules import DEFAULT_OUTCOME, OUTCOME_SCRIPT_RULES, rule_for_outcome


class OutcomeScriptRulesTest(unittest.TestCase):
    def test_all_expected_outcomes_have_required_fields(self):
        expected_outcomes = {
            "SALE",
            "CALLBACK",
            "SOFT_REJECTION",
            "HARD_REJECTION",
            "DO_NOT_CALL",
            "IN_PROGRESS",
        }
        required_fields = {
            "outcome_strategy",
            "script_template_id",
            "script_title",
            "next_action",
            "allows_product_pitch",
        }

        self.assertEqual(expected_outcomes, set(OUTCOME_SCRIPT_RULES))
        for outcome in expected_outcomes:
            self.assertTrue(required_fields.issubset(OUTCOME_SCRIPT_RULES[outcome]))

    def test_next_action_mapping(self):
        self.assertEqual(
            "SEND_APPLICATION_OR_VERIFY_DOCUMENTS",
            rule_for_outcome("SALE")["next_action"],
        )
        self.assertEqual("SCHEDULE_CALLBACK", rule_for_outcome("CALLBACK")["next_action"])
        self.assertEqual(
            "SEND_INFORMATION_AND_FOLLOW_UP",
            rule_for_outcome("SOFT_REJECTION")["next_action"],
        )
        self.assertEqual("MARK_NOT_INTERESTED", rule_for_outcome("HARD_REJECTION")["next_action"])
        self.assertEqual(
            "ADD_TO_DO_NOT_CALL_LIST",
            rule_for_outcome("DO_NOT_CALL")["next_action"],
        )
        self.assertEqual(
            "CONTINUE_NEEDS_DISCOVERY",
            rule_for_outcome("IN_PROGRESS")["next_action"],
        )

    def test_do_not_call_and_hard_rejection_do_not_pitch(self):
        self.assertFalse(rule_for_outcome("DO_NOT_CALL")["allows_product_pitch"])
        self.assertFalse(rule_for_outcome("HARD_REJECTION")["allows_product_pitch"])

    def test_unknown_outcome_falls_back_to_in_progress(self):
        self.assertEqual(rule_for_outcome(DEFAULT_OUTCOME), rule_for_outcome("UNKNOWN"))


if __name__ == "__main__":
    unittest.main()
