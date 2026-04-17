#!/usr/bin/env python3
"""
load_data.py
============
Loads mock data (customers, offers, calls) into MongoDB telesales database.
Runs as a Docker init container after MongoDB ReplicaSet is ready.

Idempotent: uses upsert on natural key, safe to re-run.

Expected files in /data/:
  customers.csv  — customer profiles
  offers.csv     — loan/card offers per customer-campaign pair
  calls.csv      — individual telesales call records
"""

import os
import ast
import json
import sys
import pandas as pd
from pymongo import MongoClient, ReplaceOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/?replicaSet=rs0")
DATA_DIR  = "/data"


def load_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"[ERROR] File not found: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    # Replace NaN with None (NaN is not valid in BSON)
    return df.where(pd.notna(df), other=None)


def upsert_bulk(collection, docs, pk_field):
    ops = [
        ReplaceOne({pk_field: doc[pk_field]}, doc, upsert=True)
        for doc in docs
        if doc.get(pk_field) is not None
    ]
    result = collection.bulk_write(ops, ordered=False)
    print(f"  upserted: {result.upserted_count}  updated: {result.modified_count}  "
          f"total in collection: {collection.count_documents({})}")


def main():
    print("Connecting to MongoDB ...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    client.admin.command("ping")
    print("Connected.\n")

    db = client["telesales"]

    # ── 1. Customers ───────────────────────────────────────────────────────────
    print("[1/3] Loading customers.csv ...")
    df = load_csv("customers.csv")
    df["age"]                  = pd.to_numeric(df["age"],            errors="coerce")
    df["monthly_income"]       = pd.to_numeric(df["monthly_income"], errors="coerce")
    df["credit_score"]         = pd.to_numeric(df["credit_score"],   errors="coerce")
    df["is_existing_customer"] = df["is_existing_customer"].map(
        lambda x: bool(x) if x is not None else None
    )
    upsert_bulk(db["cust"], df.to_dict("records"), "customer_id")

    # ── 2. Offers ──────────────────────────────────────────────────────────────
    print("[2/3] Loading offers.csv ...")
    df = load_csv("offers.csv")
    df["decile_group"]  = pd.to_numeric(df["decile_group"],  errors="coerce")
    df["loan_amount"]   = pd.to_numeric(df["loan_amount"],   errors="coerce")
    df["interest_rate"] = pd.to_numeric(df["interest_rate"], errors="coerce")
    upsert_bulk(db["offer"], df.to_dict("records"), "offer_id")

    # ── 3. Calls ───────────────────────────────────────────────────────────────
    print("[3/3] Loading calls.csv ...")
    df = load_csv("calls.csv")

    # calls.csv has duplicate "call_id" column:
    #   col[0] = sequential CALL-001 style (original source field)
    #   col[1] = UUID (renamed from unique_id — the real PK)
    # Keep only the UUID one.
    if "call_id.1" in df.columns:
        df = df.drop(columns=["call_id"]).rename(columns={"call_id.1": "call_id"})

    df["talk_time_seconds"]      = pd.to_numeric(df["talk_time_seconds"],      errors="coerce")
    df["previous_contact_count"] = pd.to_numeric(df["previous_contact_count"], errors="coerce")

    # call_code is stored as JSON array string: '["OPENING", "SUCCESSFUL_SALE"]'
    def parse_call_code(val):
        if val is None:
            return []
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            try:
                return ast.literal_eval(val)  # fallback for Python repr format
            except Exception:
                return []

    df["call_code"] = df["call_code"].apply(parse_call_code)

    upsert_bulk(db["call_logs"], df.to_dict("records"), "call_id")

    print("\nData loading completed successfully.")
    client.close()


if __name__ == "__main__":
    main()
