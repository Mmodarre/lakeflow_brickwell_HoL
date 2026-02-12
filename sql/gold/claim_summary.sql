-- Gold: Comprehensive claim summary with line aggregations and assessment outcome
-- Joins current claim records with aggregated claim lines and latest assessment
-- SCD2: filters __END_AT IS NULL on claim and claim_line for current versions only

WITH current_claims AS (
  SELECT *
  FROM {catalog}.{silver_schema_claim}.claim
  WHERE __END_AT IS NULL
),

line_agg AS (
  SELECT
    claim_id,
    COUNT(*) AS line_count,
    SUM(charge_amount) AS total_line_charge,
    SUM(benefit_amount) AS total_line_benefit,
    SUM(gap_amount) AS total_line_gap
  FROM {catalog}.{silver_schema_claim}.claim_line
  WHERE __END_AT IS NULL
  GROUP BY claim_id
),

latest_assessment AS (
  SELECT
    claim_id,
    assessment_type,
    assessed_by,
    outcome,
    original_benefit,
    adjusted_benefit,
    assessment_date AS assessed_date
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY assessment_date DESC) AS rn
    FROM {catalog}.{silver_schema_claim}.claim_assessment
  )
  WHERE rn = 1
)

SELECT
  c.claim_id,
  c.claim_number,
  c.policy_id,
  c.member_id,
  c.claim_type,
  c.claim_status,
  c.service_date,
  c.lodgement_date,
  c.assessment_date,
  c.payment_date,
  c.total_charge,
  c.total_benefit,
  c.total_gap,
  c.excess_applied,
  c.co_payment_applied,
  COALESCE(cl.line_count, 0) AS line_count,
  COALESCE(cl.total_line_charge, 0) AS total_line_charge,
  COALESCE(cl.total_line_benefit, 0) AS total_line_benefit,
  COALESCE(cl.total_line_gap, 0) AS total_line_gap,
  ca.assessment_type,
  ca.assessed_by,
  ca.outcome AS assessment_outcome,
  ca.original_benefit AS assessed_original_benefit,
  ca.adjusted_benefit AS assessed_adjusted_benefit,
  DATEDIFF(c.assessment_date, c.lodgement_date) AS days_to_assess,
  DATEDIFF(c.payment_date, c.assessment_date) AS days_to_pay,
  c.claim_channel,
  c.is_fraud,
  c.fraud_type
FROM current_claims c
LEFT JOIN line_agg cl ON c.claim_id = cl.claim_id
LEFT JOIN latest_assessment ca ON c.claim_id = ca.claim_id
