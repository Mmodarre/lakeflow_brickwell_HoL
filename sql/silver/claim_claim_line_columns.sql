-- Column ordering for claim.claim_line silver table
-- Source: v_claim_line_with_sequence (after epoch day conversion and sequence timestamp)
SELECT
  claim_line_id,
  claim_id,
  line_number,
  item_code,
  item_description,
  clinical_category_id,
  benefit_category_id,
  service_date,
  quantity,
  CAST(charge_amount AS DECIMAL(10, 2)) AS charge_amount,
  CAST(schedule_fee AS DECIMAL(10, 2)) AS schedule_fee,
  CAST(benefit_amount AS DECIMAL(10, 2)) AS benefit_amount,
  CAST(gap_amount AS DECIMAL(10, 2)) AS gap_amount,
  line_status,
  rejection_reason_id,
  provider_id,
  provider_number,
  tooth_number,
  body_part,
  _sequence_timestamp,
  -- Bronze metadata columns
  _source_file_path,
  _source_file_name,
  _processing_timestamp
FROM STREAM(v_claim_line_with_sequence)
