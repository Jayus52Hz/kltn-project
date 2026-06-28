from __future__ import annotations

from pathlib import Path
import unicodedata

from docx import Document


ROOT = Path(__file__).resolve().parents[1]

DOCX = ROOT / "docs" / "reports" / "Report KLTN - 22133056 - Nguyen Quoc Thinh - 08-06 - v3 chuong1 ly thuyet chuyen sau.docx"
OUT = ROOT / "outputs" / "review" / "review_output" / "v3_chapter1_theory_verification.txt"


def strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    return "".join(
        char
        for char in unicodedata.normalize("NFD", text)
        if unicodedata.category(char) != "Mn"
    ).lower()


def main() -> None:
    doc = Document(str(DOCX))
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]

    start = next(
        i
        for i, text in enumerate(paragraphs)
        if i > 100 and strip_accents(text).startswith("chuong 1:")
    )
    end = next(
        i
        for i, text in enumerate(paragraphs[start + 1 :], start + 1)
        if strip_accents(text).startswith("chuong 2:")
    )
    chapter1 = "\n".join(paragraphs[start:end])
    normalized = strip_accents(chapter1)

    checks = {
        "Lakehouse distinction": "data warehouse, data lake va data lakehouse khac nhau",
        "MongoDB Change Streams": "change streams cung cap luong su kien thay doi",
        "Debezium": "debezium hoat dong nhu mot lop source connector",
        "Kafka": "kafka to chuc du lieu thanh event",
        "Spark": "spark cung cap mo hinh xu ly dua tren dataframe",
        "Iceberg": "iceberg khong phai la database doc lap",
        "MinIO/Object Storage": "object storage to chuc du lieu theo bucket",
        "Airflow": "airflow mo hinh hoa workflow bang dag",
        "Superset": "superset la lop visualization/bi",
        "BigQuery/Looker Studio": "bigquery la kho du lieu phan tich serverless",
        "Docker Compose": "docker compose la cong cu dinh nghia va chay ung dung",
    }

    citation_checks = ["[4]", "[5]", "[6]", "[7]", "[8]", "[9]", "[10]", "[11]", "[30]", "[31]", "[32]", "[33]"]
    note_count = normalized.count("note anh")

    lines = [
        f"DOCX: {DOCX}",
        f"Chapter 1 paragraphs checked: {end - start}",
        "",
        "Detailed theory insertion checks:",
    ]
    all_ok = True
    for label, needle in checks.items():
        ok = needle in normalized
        all_ok = all_ok and ok
        lines.append(f"- {label}: {'OK' if ok else 'MISSING'}")

    lines.extend(["", "Citation token checks:"])
    for token in citation_checks:
        ok = token in chapter1
        all_ok = all_ok and ok
        lines.append(f"- {token}: {'OK' if ok else 'MISSING'}")

    note_ok = note_count >= 10
    all_ok = all_ok and note_ok
    lines.extend(
        [
            "",
            f"Image note count in Chapter 1: {note_count} ({'OK' if note_ok else 'LOW'})",
            f"Overall: {'PASS' if all_ok else 'FAIL'}",
        ]
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
