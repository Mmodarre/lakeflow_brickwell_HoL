-- Column ordering for billing.invoice silver table
-- Source: v_invoice_with_sequence (after UUID conversion and sequence timestamp)
SELECT
  invoice_id,
  invoice_number,
  policy_id,
  invoice_date,
  due_date,
  period_start,
  period_end,
  invoice_status,
  gross_premium,
  lhc_loading_amount,
  age_discount_amount,
  rebate_amount,
  other_adjustments,
  net_amount,
  gst_amount,
  total_amount,
  paid_amount,
  balance_due,
  retry_attempts,
  next_retry_date,
  arrears_created,
  _sequence_timestamp,
  -- Bronze metadata columns
  _source_file_path,
  _source_file_name,
  _processing_timestamp
FROM STREAM(v_invoice_with_sequence)
