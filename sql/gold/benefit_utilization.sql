-- Gold: Benefit utilization analysis per member, benefit category, and year
-- Aggregates usage amounts, counts, and calculates utilization percentage
-- SCD2: filters __END_AT IS NULL on claim for current versions only
-- benefit_usage is append-only (no SCD2 filtering needed)

WITH current_claims AS (
  SELECT claim_id, claim_status
  FROM {catalog}.{silver_schema_claim}.claim
  WHERE __END_AT IS NULL
),

usage_agg AS (
  SELECT
    bu.policy_id,
    bu.member_id,
    bu.benefit_category_id,
    bu.benefit_year,
    bu.limit_type,
    SUM(bu.usage_amount) AS total_usage_amount,
    SUM(bu.usage_count) AS total_usage_count,
    MAX(bu.annual_limit) AS annual_limit,
    MIN(bu.remaining_limit) AS remaining_limit,
    COUNT(DISTINCT bu.claim_id) AS claim_count,
    COUNT(DISTINCT CASE WHEN cc.claim_status IN ('Assessed', 'Paid') THEN bu.claim_id END) AS approved_claim_count,
    MIN(bu.usage_date) AS first_usage_date,
    MAX(bu.usage_date) AS last_usage_date
  FROM {catalog}.{silver_schema_claim}.benefit_usage bu
  LEFT JOIN current_claims cc ON bu.claim_id = cc.claim_id
  GROUP BY bu.policy_id, bu.member_id, bu.benefit_category_id, bu.benefit_year, bu.limit_type
)

SELECT
  policy_id,
  member_id,
  benefit_category_id,
  benefit_year,
  limit_type,
  total_usage_amount,
  total_usage_count,
  annual_limit,
  remaining_limit,
  claim_count,
  approved_claim_count,
  first_usage_date,
  last_usage_date,
  CASE
    WHEN annual_limit > 0 AND limit_type = 'Dollar'
    THEN ROUND(total_usage_amount / annual_limit * 100, 2)
    ELSE NULL
  END AS utilization_pct
FROM usage_agg
