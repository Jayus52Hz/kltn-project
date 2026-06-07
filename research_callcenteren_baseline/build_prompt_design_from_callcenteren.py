"""
Build a report-ready analysis that connects CallCenterEN characteristics to the
prompt/data-design rules used for the primary telesales dataset.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
BASELINE_CSV = (
    ROOT
    / "92k-real-world-call-center-scripts-english"
    / "prepared_subset"
    / "baseline_analysis_sample.csv"
)
SUMMARY_JSON = OUTPUT_DIR / "dataset_comparison_summary.json"
OUTPUT_MD = OUTPUT_DIR / "callcenteren_prompt_design_analysis.md"
OUTPUT_JSON = OUTPUT_DIR / "callcenteren_prompt_design_analysis.json"


KEYWORD_GROUPS = {
    "opening": [
        "thank you for calling",
        "thanks for calling",
        "hello",
        "hi",
        "good morning",
        "good afternoon",
        "how can i help",
        "how may i help",
    ],
    "identity_verification": [
        "date of birth",
        "address",
        "zip code",
        "phone number",
        "first name",
        "last name",
        "verify",
        "confirm",
    ],
    "needs_analysis": [
        "currently have",
        "what kind",
        "how many",
        "are you looking",
        "do you have",
        "tell me",
        "coverage",
        "household",
    ],
    "product_or_offer": [
        "quote",
        "policy",
        "plan",
        "benefit",
        "premium",
        "coverage",
        "offer",
        "discount",
    ],
    "fee_discussion": [
        "premium",
        "payment",
        "pay",
        "cost",
        "price",
        "dollar",
        "monthly",
        "deductible",
    ],
    "objection_or_rejection": [
        "not interested",
        "no thank",
        "already have",
        "busy",
        "call back",
        "remove me",
        "don't call",
        "do not call",
    ],
    "handoff_or_followup": [
        "transfer",
        "specialist",
        "send",
        "email",
        "text",
        "call back",
        "follow up",
    ],
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def pct(count: int, total: int) -> float:
    return round(count * 100.0 / total, 2) if total else 0.0


def analyze(rows: list[dict[str, str]]) -> dict[str, Any]:
    total = len(rows)
    domain_counter = Counter(row.get("source_domain", "unknown") for row in rows)
    direction_counter = Counter(row.get("call_direction", "unknown") for row in rows)
    source_counter = Counter(row.get("source_zip", "unknown") for row in rows)
    pii_counter: Counter[str] = Counter()
    keyword_hits: dict[str, int] = {}
    examples: dict[str, str] = {}

    char_counts = []
    word_counts = []
    durations = []
    confidences = []
    pii_counts = []

    for row in rows:
        text = row.get("text", "")
        lower = text.lower()
        char_counts.append(int(float(row.get("char_count") or 0)))
        word_counts.append(int(float(row.get("word_count") or 0)))
        durations.append(float(row.get("audio_duration") or 0))
        confidences.append(float(row.get("confidence") or 0))
        pii_counts.append(int(float(row.get("pii_token_count") or 0)))
        for pii_type in [item for item in row.get("pii_types", "").split("|") if item]:
            pii_counter[pii_type] += 1
        for group, keywords in KEYWORD_GROUPS.items():
            if any(keyword in lower for keyword in keywords):
                keyword_hits[group] = keyword_hits.get(group, 0) + 1
                examples.setdefault(group, text[:700])

    def avg(values: list[float | int]) -> float:
        return round(sum(values) / len(values), 4) if values else 0.0

    return {
        "rows": total,
        "domain_distribution": domain_counter.most_common(),
        "direction_distribution": direction_counter.most_common(),
        "source_distribution": source_counter.most_common(),
        "avg_chars": avg(char_counts),
        "avg_words": avg(word_counts),
        "avg_duration": avg(durations),
        "avg_confidence": avg(confidences),
        "avg_pii_tokens": avg(pii_counts),
        "top_pii_types": pii_counter.most_common(20),
        "keyword_coverage": {
            group: {
                "rows": count,
                "percent": pct(count, total),
            }
            for group, count in sorted(keyword_hits.items(), key=lambda item: item[1], reverse=True)
        },
        "prompt_design_mapping": [
            {
                "callcenteren_observation": "Transcript có lời chào, IVR hoặc câu mở đầu của agent/customer.",
                "prompt_rule_for_primary_dataset": "Bắt buộc transcript sinh ra có đoạn OPENING rõ ràng, có speaker Agent/Customer.",
                "primary_dataset_field_or_label": "call_transcript, OPENING",
            },
            {
                "callcenteren_observation": "Nhiều cuộc gọi có xác minh thông tin như tên, tuổi, địa chỉ, số điện thoại, ngày sinh.",
                "prompt_rule_for_primary_dataset": "Sinh customer profile và đưa một phần thông tin vào hội thoại, sau đó pipeline phải xử lý PII.",
                "primary_dataset_field_or_label": "customer fields, PII masking",
            },
            {
                "callcenteren_observation": "Agent thường hỏi nhu cầu, tình trạng hiện tại, coverage, household hoặc khả năng chi trả.",
                "prompt_rule_for_primary_dataset": "Prompt phải yêu cầu đoạn NEEDS_ANALYSIS trước khi pitch sản phẩm.",
                "primary_dataset_field_or_label": "NEEDS_ANALYSIS",
            },
            {
                "callcenteren_observation": "Các cuộc gọi bảo hiểm/customer service có thảo luận quote, premium, payment, plan, benefit.",
                "prompt_rule_for_primary_dataset": "Prompt sinh offer/product và yêu cầu transcript chứa PRODUCT_PITCH/FEE_DISCUSSION khi phù hợp.",
                "primary_dataset_field_or_label": "offer, product_name, PRODUCT_PITCH, FEE_DISCUSSION",
            },
            {
                "callcenteren_observation": "Khách hàng có thể đồng ý thụ động, phản đối, từ chối, yêu cầu gọi lại hoặc kết thúc đột ngột.",
                "prompt_rule_for_primary_dataset": "Prompt phải tạo outcome đa dạng: WARM_LEAD, SOFT_REJECTION, HARD_REJECTION, DO_NOT_CALL_REQUEST, SUDDEN_HANG_UP.",
                "primary_dataset_field_or_label": "call_code, outcome flags",
            },
            {
                "callcenteren_observation": "Dữ liệu thực tế có inbound/outbound và nhiều domain.",
                "prompt_rule_for_primary_dataset": "Dataset chính giữ domain telesales tài chính nhưng vẫn mô phỏng các kịch bản lead source, campaign và sản phẩm khác nhau.",
                "primary_dataset_field_or_label": "lead_source, campaign_id, product_name",
            },
            {
                "callcenteren_observation": "Có metadata về audio_duration và confidence.",
                "prompt_rule_for_primary_dataset": "Dataset chính cần có talk_time_seconds; ASR confidence được ghi là hướng mở rộng nếu có audio/ASR thật.",
                "primary_dataset_field_or_label": "talk_time_seconds",
            },
        ],
    }


def write_markdown(result: dict[str, Any]) -> None:
    lines = [
        "# Phân tích CallCenterEN để thiết kế prompt và dataset chính",
        "",
        "## Mục đích",
        "",
        "CallCenterEN được sử dụng như bước nghiên cứu nền trước khi mô tả và bảo vệ dataset chính. "
        "Mạch phương pháp là: phân tích transcript call center thực tế -> trích xuất đặc trưng nghiệp vụ/ngôn ngữ -> chuyển thành nguyên tắc prompt -> sinh dataset telesales chính có schema và nhãn phù hợp.",
        "",
        "## Thống kê từ 3,000 mẫu baseline",
        "",
        f"- Số mẫu phân tích: {result['rows']:,}",
        f"- Độ dài trung bình: {result['avg_chars']:,} ký tự, {result['avg_words']:,} từ",
        f"- Thời lượng trung bình: {result['avg_duration']:,} giây",
        f"- ASR confidence trung bình: {result['avg_confidence']:,}",
        f"- PII token trung bình: {result['avg_pii_tokens']:,}",
        "",
        "### Phân bố domain",
        "",
        "| Domain | Số mẫu |",
        "|---|---:|",
    ]
    for domain, count in result["domain_distribution"]:
        lines.append(f"| `{domain}` | {count:,} |")
    lines.extend(["", "### Phân bố hướng cuộc gọi", "", "| Hướng | Số mẫu |", "|---|---:|"])
    for direction, count in result["direction_distribution"]:
        lines.append(f"| `{direction}` | {count:,} |")
    lines.extend(["", "### Đặc trưng hội thoại phát hiện bằng keyword groups", "", "| Nhóm đặc trưng | Số mẫu | Tỷ lệ |", "|---|---:|---:|"])
    for group, value in result["keyword_coverage"].items():
        lines.append(f"| `{group}` | {value['rows']:,} | {value['percent']}% |")
    lines.extend(["", "### Mapping từ CallCenterEN sang prompt dataset chính", "", "| Quan sát từ CallCenterEN | Quy tắc prompt cho dataset chính | Trường/nhãn trong dataset chính |", "|---|---|---|"])
    for item in result["prompt_design_mapping"]:
        lines.append(
            f"| {item['callcenteren_observation']} | {item['prompt_rule_for_primary_dataset']} | `{item['primary_dataset_field_or_label']}` |"
        )
    lines.extend([
        "",
        "## Kết luận phương pháp",
        "",
        "Phần 3,000 mẫu CallCenterEN không phải tập training pseudo-label. Nó là tập phân tích đặc trưng để chứng minh dataset chính không được sinh tùy tiện. "
        "Từ các đặc trưng như opening, xác minh thông tin, needs analysis, pitch, fee discussion, objection/rejection, PII và duration, prompt sinh dataset chính được thiết kế để tạo transcript có cấu trúc nghiệp vụ và có nhãn call_code.",
        "",
        "Phần 300 pseudo-label là một thí nghiệm riêng, nhỏ hơn, dùng để kiểm tra việc CallCenterEN có thể tham gia training như auxiliary corpus. "
        "Do đó cần phân biệt rõ: 3,000 mẫu dùng cho phân tích thiết kế dataset/prompt; 300 mẫu dùng cho pilot weak-label training.",
    ])
    OUTPUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    rows = read_csv(BASELINE_CSV)
    result = analyze(rows)
    OUTPUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(result)
    print(f"Wrote: {OUTPUT_JSON.relative_to(ROOT)}")
    print(f"Wrote: {OUTPUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
