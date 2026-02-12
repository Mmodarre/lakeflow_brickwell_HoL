-- Column ordering for claim.benefit_usage silver table
-- Source: v_benefit_usage_prepared (after epoch day/timestamp conversion)
SELECT
  benefit_usage_id,
  policy_id,
  member_id,
  claim_id,
  benefit_category_id,
  benefit_year,
  usage_date,
  CAST(usage_amount AS DECIMAL(10, 2)) AS usage_amount,
  usage_count,
  CAST(annual_limit AS DECIMAL(10, 2)) AS annual_limit,
  CAST(remaining_limit AS DECIMAL(10, 2)) AS remaining_limit,
  limit_type,
  created_at,
  -- Bronze metadata columns
  _source_file_path,
  _source_file_name,
  _processing_timestamp
FROM STREAM(v_benefit_usage_prepared)
