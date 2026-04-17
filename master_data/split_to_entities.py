"""
split_to_entities.py
====================
Merges all transcript_batch*.json files and splits into 3 normalized CSV files:

    customers.csv  —  1 row per unique customer       (10 rows)
    offers.csv     —  1 row per customer x campaign   (57 rows)
    calls.csv      —  1 row per individual call        (~23,447 rows)

Relationship:  customer (1) --> (many) offers (1) --> (many) calls

Keys generated
--------------
  customer_id : original CUST-#### field, profile taken from first occurrence
                (source data has inconsistent profiles per ID — mock data issue)
  offer_id    : new sequential key OFFER-00001 … assigned to each unique
                (customer_id, campaign_id) pair, sorted deterministically
  call_id     : unique_id from source (UUID, globally unique)

Usage
-----
    python split_to_entities.py

Output is written to ./output/ relative to this script.
"""

import json
import glob
import pandas as pd
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DATA_GLOB  = str(SCRIPT_DIR / 'transcript_batch*.json')
OUTPUT_DIR = SCRIPT_DIR / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)

# ── 1. Load all JSON files ─────────────────────────────────────────────────────
print('Loading JSON files...')
all_records = []
for filepath in sorted(glob.glob(DATA_GLOB)):
    with open(filepath, encoding='utf-8') as f:
        all_records.extend(json.load(f))

print(f'Total records loaded: {len(all_records):,}')
df = pd.DataFrame(all_records)

# ── 2. Rename unique_id → call_id ─────────────────────────────────────────────
df = df.rename(columns={'unique_id': 'call_id'})

# ── 3. Generate offer_id ───────────────────────────────────────────────────────
# An offer = a specific customer being targeted by a specific campaign.
# Sort pairs deterministically so offer_id is stable across re-runs.
offer_pairs = (
    df[['customer_id', 'campaign_id']]
    .drop_duplicates()
    .sort_values(['customer_id', 'campaign_id'])
    .reset_index(drop=True)
)
offer_pairs['offer_id'] = offer_pairs.index.map(lambda i: f'OFFER-{i+1:05d}')

df = df.merge(offer_pairs, on=['customer_id', 'campaign_id'], how='left')

# ── 4. Build customers.csv ─────────────────────────────────────────────────────
# Profile fields — take first occurrence per customer_id (mock data has
# inconsistent profiles; first occurrence is the most stable anchor).
CUSTOMER_COLS = [
    'customer_id',
    'full_name',
    'age',
    'gender',
    'phone_number',      # PII — will be masked in Silver layer
    'national_id',       # PII — will be masked in Silver layer
    'address',
    'employment_status',
    'monthly_income',
    'credit_score',
    'is_existing_customer',
]

customers_df = (
    df[CUSTOMER_COLS]
    .drop_duplicates(subset='customer_id', keep='first')
    .sort_values('customer_id')
    .reset_index(drop=True)
)
customers_path = OUTPUT_DIR / 'customers.csv'
customers_df.to_csv(customers_path, index=False)
print(f'customers.csv  -> {customers_path}  ({len(customers_df):,} rows)')

# ── 5. Build offers.csv ────────────────────────────────────────────────────────
# Offer-level fields — stable per (customer_id, campaign_id) pair.
# Take first occurrence since product/rate info should not vary within an offer.
OFFER_COLS = [
    'offer_id',
    'customer_id',       # FK -> customers
    'campaign_id',
    'product_name',
    'lead_source',
    'decile_group',
    'loan_amount',
    'interest_rate',
]

offers_df = (
    df[OFFER_COLS]
    .drop_duplicates(subset='offer_id', keep='first')
    .sort_values('offer_id')
    .reset_index(drop=True)
)
offers_path = OUTPUT_DIR / 'offers.csv'
offers_df.to_csv(offers_path, index=False)
print(f'offers.csv     -> {offers_path}  ({len(offers_df):,} rows)')

# ── 6. Build calls.csv ─────────────────────────────────────────────────────────
# One row per call. call_codes stored as JSON array string for downstream parsing.
CALL_COLS = [
    'call_id',           # PK (UUID from source)
    'offer_id',          # FK -> offers
    'agent_id',
    'call_timestamp',
    'call_status',
    'talk_time_seconds',
    'previous_contact_count',
    'call_code',         # list -> stored as JSON string
    'call_transcript',   # raw text -> NLP Silver layer
]

calls_df = df[CALL_COLS].copy()

# Serialize call_code list to JSON string so CSV stays flat
calls_df['call_code'] = calls_df['call_code'].apply(
    lambda x: json.dumps(x) if isinstance(x, list) else x
)

calls_path = OUTPUT_DIR / 'calls.csv'
calls_df.to_csv(calls_path, index=False)
print(f'calls.csv      -> {calls_path}  ({len(calls_df):,} rows)')

# ── 7. Validate relationships ──────────────────────────────────────────────────
print()
print('=== Relationship validation ===')

calls_per_offer = calls_df.groupby('offer_id').size()
offers_per_cust = offers_df.groupby('customer_id').size()

print(f'Customers         : {len(customers_df):,}')
print(f'Offers            : {len(offers_df):,}')
print(f'Calls             : {len(calls_df):,}')
print()
print(f'Offers per customer — min: {offers_per_cust.min()}  '
      f'max: {offers_per_cust.max()}  '
      f'avg: {offers_per_cust.mean():.1f}')
print(f'Calls per offer   — min: {calls_per_offer.min()}  '
      f'max: {calls_per_offer.max()}  '
      f'avg: {calls_per_offer.mean():.1f}')
print()

# FK integrity check
orphan_offers = set(offers_df['customer_id']) - set(customers_df['customer_id'])
orphan_calls  = set(calls_df['offer_id']) - set(offers_df['offer_id'])
print(f'FK check — orphan offers (no customer): {len(orphan_offers)}')
print(f'FK check — orphan calls  (no offer)   : {len(orphan_calls)}')
print()
print('Done.')
