DROP VIEW IF EXISTS `project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance`;

CREATE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance`
OPTIONS(expiration_timestamp=TIMESTAMP '2026-08-20 00:00:00 UTC') AS
SELECT
  f.call_id,
  f.customer_id,
  f.offer_id,
  f.date_key,
  d.full_date,
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  d.week_of_year,
  d.day_of_month,
  d.day_of_week,
  d.day_name,
  d.is_weekend,
  f.agent_id,
  f.call_status,
  f.talk_time_seconds,
  f.talk_time_band,
  f.previous_contact_count,
  f.call_code,
  f.has_successful_sale,
  f.has_hard_rejection,
  f.has_soft_rejection,
  f.has_do_not_call,
  f.has_objection,
  f.outcome_category,
  c.age,
  c.age_group,
  c.gender,
  c.employment_status,
  c.monthly_income,
  c.income_band,
  c.credit_score,
  c.credit_tier,
  c.is_existing_customer,
  o.campaign_id,
  o.product_name,
  o.product_category,
  o.lead_source,
  o.decile_group,
  o.loan_amount,
  o.interest_rate
FROM `project-ef0c6db5-0765-4391-845.kltn0710.fact_telesales_calls` AS f
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_date` AS d
  USING (date_key)
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_customer` AS c
  USING (customer_id)
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_offer` AS o
  USING (offer_id);

DROP VIEW IF EXISTS `project-ef0c6db5-0765-4391-845.kltn0710.vw_customer_outcome_scripts`;

CREATE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_customer_outcome_scripts`
OPTIONS(expiration_timestamp=TIMESTAMP '2026-08-20 00:00:00 UTC') AS
SELECT
  s.script_id,
  s.call_id,
  s.customer_id,
  s.offer_id,
  s.outcome_category,
  s.outcome_strategy,
  s.script_template_id,
  s.script_version,
  s.script_title,
  s.opening_line,
  s.main_pitch,
  s.objection_response,
  s.next_action,
  s.closing_line,
  s.variables_json,
  f.date_key,
  d.full_date,
  f.agent_id,
  f.call_status,
  f.talk_time_seconds,
  f.talk_time_band,
  f.previous_contact_count,
  f.call_code,
  c.age,
  c.age_group,
  c.gender,
  c.employment_status,
  c.monthly_income,
  c.income_band,
  c.credit_score,
  c.credit_tier,
  c.is_existing_customer,
  o.campaign_id,
  o.product_name,
  o.product_category,
  o.lead_source,
  o.decile_group,
  o.loan_amount,
  o.interest_rate
FROM `project-ef0c6db5-0765-4391-845.kltn0710.customer_outcome_scripts` AS s
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.fact_telesales_calls` AS f
  ON s.call_id = f.call_id
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_date` AS d
  ON f.date_key = d.date_key
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_customer` AS c
  ON s.customer_id = c.customer_id
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_offer` AS o
  ON s.offer_id = o.offer_id;

DROP VIEW IF EXISTS `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_labeled`;

CREATE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_labeled`
OPTIONS(expiration_timestamp=TIMESTAMP '2026-08-20 00:00:00 UTC') AS
SELECT
  dataset_name,
  text_hash,
  source_zip,
  source_domain,
  call_direction,
  audio_duration,
  asr_confidence,
  word_count,
  char_count,
  pii_token_count,
  pii_types,
  model_call_code,
  model_call_code_confidence,
  model_name,
  has_existing_pseudo_label,
  pseudo_call_code_existing,
  pseudo_label_confidence_existing,
  should_use_for_training,
  call_code_source,
  label_confidence,
  call_code
FROM `project-ef0c6db5-0765-4391-845.kltn0710.callcenteren_labeled`;

DROP VIEW IF EXISTS `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_performance`;

CREATE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_performance`
OPTIONS(expiration_timestamp=TIMESTAMP '2026-08-20 00:00:00 UTC') AS
SELECT
  f.callcenter_call_id,
  f.source_key,
  f.model_key,
  s.source_zip,
  s.source_domain,
  s.call_direction,
  m.model_name,
  f.audio_duration,
  f.asr_confidence,
  f.word_count,
  f.char_count,
  f.pii_token_count,
  f.pii_types,
  f.model_call_code,
  f.model_call_code_confidence,
  f.has_existing_pseudo_label,
  f.pseudo_call_code_existing,
  f.pseudo_label_confidence_existing,
  f.should_use_for_training,
  f.call_code_source,
  f.label_confidence,
  f.call_code
FROM `project-ef0c6db5-0765-4391-845.kltn0710.fact_callcenter_calls` AS f
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_callcenter_source` AS s
  USING (source_key)
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_callcenter_model` AS m
  USING (model_key);

DROP VIEW IF EXISTS `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_call_codes`;

CREATE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_callcenteren_call_codes`
OPTIONS(expiration_timestamp=TIMESTAMP '2026-08-20 00:00:00 UTC') AS
SELECT
  b.callcenter_call_id,
  b.call_code_key,
  c.call_code,
  s.source_zip,
  s.source_domain,
  s.call_direction,
  m.model_name,
  f.audio_duration,
  f.asr_confidence,
  f.word_count,
  f.char_count,
  f.pii_token_count,
  f.should_use_for_training,
  b.label_confidence
FROM `project-ef0c6db5-0765-4391-845.kltn0710.bridge_callcenter_call_code` AS b
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.fact_callcenter_calls` AS f
  ON b.callcenter_call_id = f.callcenter_call_id
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_callcenter_source` AS s
  ON b.source_key = s.source_key
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_callcenter_model` AS m
  ON b.model_key = m.model_key
LEFT JOIN `project-ef0c6db5-0765-4391-845.kltn0710.dim_call_code` AS c
  ON b.call_code_key = c.call_code_key;
