-- Gold: Member Claims Risk & Activity Analysis
-- Grain: one row per claim per member (current SCD2 version only)
--
-- Business purpose:
--   Provides a complete analytical view of each member's claiming behaviour over time.
--   Used for risk profiling, fraud detection, claims operations performance,
--   and actuarial analysis. Window functions calculate per-member trends and
--   cross-population comparisons without pre-aggregation.
--
-- Window function patterns used:
--   ROW_NUMBER  - claim sequencing per member
--   LAG / LEAD  - inter-claim gap analysis (days between claims)
--   SUM OVER    - cumulative running totals per member
--   AVG OVER    - 3-claim moving average for trend detection
--   COUNT/SUM/AVG OVER (unbounded) - member lifetime aggregates
--   RANK        - cost ranking within member's own history
--   PERCENT_RANK - cross-member cost percentile ranking

WITH current_claims AS (
  SELECT *
  FROM {catalog}.{silver_schema_claim}.claim
  WHERE __END_AT IS NULL
),

latest_assessment AS (
  SELECT claim_id, outcome, assessment_type
  FROM (
    SELECT
      claim_id,
      outcome,
      assessment_type,
      ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY assessment_date DESC) AS rn
    FROM {catalog}.{silver_schema_claim}.claim_assessment
  )
  WHERE rn = 1
),

current_claim_lines AS (
  SELECT
    claim_id,
    COUNT(*) AS line_count,
    SUM(charge_amount) AS total_line_charge,
    SUM(benefit_amount) AS total_line_benefit
  FROM {catalog}.{silver_schema_claim}.claim_line
  WHERE __END_AT IS NULL
  GROUP BY claim_id
),

-- Step 1: Enrich claims with joins and per-member window functions
member_enriched AS (
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
    c.provider_id,
    c.hospital_id,
    c.total_charge,
    c.total_benefit,
    c.total_gap,
    c.excess_applied,
    c.co_payment_applied,
    c.claim_channel,
    c.is_fraud,
    c.fraud_type,
    ca.outcome AS assessment_outcome,
    ca.assessment_type,
    COALESCE(cl.line_count, 0) AS line_count,

    -- === Claim sequencing per member ===
    ROW_NUMBER() OVER (
      PARTITION BY c.member_id ORDER BY c.service_date, c.claim_id
    ) AS member_claim_seq,

    -- === Inter-claim gap analysis ===
    -- Days since this member's previous claim (NULL for first claim)
    DATEDIFF(
      c.service_date,
      LAG(c.service_date) OVER (
        PARTITION BY c.member_id ORDER BY c.service_date, c.claim_id
      )
    ) AS days_since_prev_claim,

    -- Days until this member's next claim (NULL for latest claim)
    DATEDIFF(
      LEAD(c.service_date) OVER (
        PARTITION BY c.member_id ORDER BY c.service_date, c.claim_id
      ),
      c.service_date
    ) AS days_to_next_claim,

    -- === Running financial totals per member ===
    SUM(c.total_charge) OVER (
      PARTITION BY c.member_id
      ORDER BY c.service_date, c.claim_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS member_running_total_charge,

    SUM(c.total_benefit) OVER (
      PARTITION BY c.member_id
      ORDER BY c.service_date, c.claim_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS member_running_total_benefit,

    -- === 3-claim moving average for trend detection ===
    ROUND(AVG(c.total_charge) OVER (
      PARTITION BY c.member_id
      ORDER BY c.service_date, c.claim_id
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS member_moving_avg_charge_3,

    -- === Member lifetime aggregates (full partition) ===
    COUNT(*) OVER (
      PARTITION BY c.member_id
    ) AS member_total_claims,

    SUM(c.total_charge) OVER (
      PARTITION BY c.member_id
    ) AS member_lifetime_charge,

    ROUND(AVG(c.total_charge) OVER (
      PARTITION BY c.member_id
    ), 2) AS member_avg_charge,

    -- === Cost ranking within member's own claims (1 = most expensive) ===
    RANK() OVER (
      PARTITION BY c.member_id ORDER BY c.total_charge DESC
    ) AS member_charge_rank,

    -- === Processing time metrics ===
    DATEDIFF(c.assessment_date, c.lodgement_date) AS days_to_assess,
    DATEDIFF(c.payment_date, c.assessment_date) AS days_to_pay

  FROM current_claims c
  LEFT JOIN latest_assessment ca ON c.claim_id = ca.claim_id
  LEFT JOIN current_claim_lines cl ON c.claim_id = cl.claim_id
)

-- Step 2: Apply cross-member ranking (requires pre-calculated lifetime charge)
SELECT
  claim_id,
  claim_number,
  policy_id,
  member_id,
  claim_type,
  claim_status,
  service_date,
  lodgement_date,
  assessment_date,
  payment_date,
  provider_id,
  hospital_id,
  total_charge,
  total_benefit,
  total_gap,
  excess_applied,
  co_payment_applied,
  claim_channel,
  assessment_outcome,
  assessment_type,
  line_count,

  -- Sequencing & timing
  member_claim_seq,
  days_since_prev_claim,
  days_to_next_claim,

  -- Running financials
  member_running_total_charge,
  member_running_total_benefit,
  member_moving_avg_charge_3,

  -- Member lifetime context
  member_total_claims,
  member_lifetime_charge,
  member_avg_charge,
  ROUND(total_charge - member_avg_charge, 2) AS charge_variance_from_avg,
  member_charge_rank,

  -- Processing performance
  days_to_assess,
  days_to_pay,

  -- Cross-member percentile ranking by lifetime cost (0.0 = lowest, 1.0 = highest)
  ROUND(
    PERCENT_RANK() OVER (ORDER BY member_lifetime_charge) * 100, 2
  ) AS member_cost_percentile,

  -- Risk / anomaly flags
  is_fraud,
  fraud_type,
  CASE
    WHEN days_since_prev_claim IS NOT NULL AND days_since_prev_claim < 7
    THEN true ELSE false
  END AS rapid_reclaim_flag,
  CASE
    WHEN total_charge > member_avg_charge * 2
    THEN true ELSE false
  END AS high_cost_outlier_flag,
  CASE
    WHEN member_moving_avg_charge_3 > member_avg_charge * 1.5
      AND member_claim_seq >= 3
    THEN true ELSE false
  END AS escalating_cost_flag

FROM member_enriched
