"""
Shared deterministic rules for customer outcome script generation.
"""

DEFAULT_OUTCOME = "IN_PROGRESS"
SCRIPT_TEMPLATE_VERSION = "v1"

OUTCOME_SCRIPT_RULES = {
    "SALE": {
        "outcome_strategy": "CONFIRM_AND_COMPLETE",
        "script_template_id": "outcome_sale_v1",
        "script_title": "Hoan tat ho so sau khi khach dong y",
        "next_action": "SEND_APPLICATION_OR_VERIFY_DOCUMENTS",
        "allows_product_pitch": True,
    },
    "CALLBACK": {
        "outcome_strategy": "SCHEDULE_CALLBACK",
        "script_template_id": "outcome_callback_v1",
        "script_title": "Hen goi lai cho khach hang quan tam",
        "next_action": "SCHEDULE_CALLBACK",
        "allows_product_pitch": True,
    },
    "SOFT_REJECTION": {
        "outcome_strategy": "NURTURE_WITH_INFORMATION",
        "script_template_id": "outcome_soft_rejection_v1",
        "script_title": "Nuoi duong khach hang con do du",
        "next_action": "SEND_INFORMATION_AND_FOLLOW_UP",
        "allows_product_pitch": True,
    },
    "HARD_REJECTION": {
        "outcome_strategy": "CLOSE_POLITELY",
        "script_template_id": "outcome_hard_rejection_v1",
        "script_title": "Ket thuc lich su voi khach tu choi manh",
        "next_action": "MARK_NOT_INTERESTED",
        "allows_product_pitch": False,
    },
    "DO_NOT_CALL": {
        "outcome_strategy": "SUPPRESS_CONTACT",
        "script_template_id": "outcome_do_not_call_v1",
        "script_title": "Xac nhan ngung lien he",
        "next_action": "ADD_TO_DO_NOT_CALL_LIST",
        "allows_product_pitch": False,
    },
    "IN_PROGRESS": {
        "outcome_strategy": "CONTINUE_DISCOVERY",
        "script_template_id": "outcome_in_progress_v1",
        "script_title": "Tiep tuc khai thac nhu cau",
        "next_action": "CONTINUE_NEEDS_DISCOVERY",
        "allows_product_pitch": True,
    },
}


def rule_for_outcome(outcome_category):
    return OUTCOME_SCRIPT_RULES.get(outcome_category, OUTCOME_SCRIPT_RULES[DEFAULT_OUTCOME])
