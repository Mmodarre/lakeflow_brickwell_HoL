-- Column ordering for billing.payment silver table
-- Source: v_payment_with_sequence (after UUID conversion and sequence timestamp)
SELECT
  payment_id,
  payment_number,
  policy_id,
  invoice_id,
  payment_date,
  payment_amount,
  payment_method,
  payment_status,
  bank_reference,
  _sequence_timestamp,
  -- Bronze metadata columns
  _source_file_path,
  _source_file_name,
  _processing_timestamp
FROM STREAM(v_payment_with_sequence)
