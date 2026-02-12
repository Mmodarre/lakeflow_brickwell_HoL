-- Column ordering for claim.claim_assessment silver table
-- Source: v_claim_assessment_prepared (after epoch timestamp conversion)
SELECT
  assessment_id,
  claim_id,
  assessment_type,
  assessment_date,
  assessed_by,
  CAST(original_benefit AS DECIMAL(10, 2)) AS original_benefit,
  CAST(adjusted_benefit AS DECIMAL(10, 2)) AS adjusted_benefit,
  adjustment_reason,
  waiting_period_check,
  benefit_limit_check,
  eligibility_check,
  outcome,
  notes,
  created_at,
  -- Bronze metadata columns
  _source_file_path,
  _source_file_name,
  _processing_timestamp
FROM STREAM(v_claim_assessment_prepared)
