CREATE OR REPLACE VIEW `project-ef0c6db5-0765-4391-845.kltn0710.vw_telesales_performance` AS
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
