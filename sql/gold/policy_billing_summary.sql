-- Gold: Policy billing summary with invoice and payment aggregations
-- Provides per-policy billing KPIs: totals, outstanding, overdue, payment rate
-- SCD2: filters __END_AT IS NULL on invoice and payment for current versions only

WITH current_invoices AS (
  SELECT *
  FROM {catalog}.{silver_schema_billing}.invoice
  WHERE __END_AT IS NULL
),

current_payments AS (
  SELECT *
  FROM {catalog}.{silver_schema_billing}.payment
  WHERE __END_AT IS NULL
    AND payment_status = 'Completed'
),

invoice_agg AS (
  SELECT
    policy_id,
    COUNT(*) AS total_invoices,
    SUM(total_amount) AS total_invoiced_amount,
    SUM(paid_amount) AS total_paid_amount,
    SUM(balance_due) AS total_outstanding,
    COUNT(CASE WHEN balance_due > 0 AND due_date < current_date() THEN 1 END) AS overdue_count,
    COUNT(CASE WHEN arrears_created = true THEN 1 END) AS arrears_count,
    MIN(invoice_date) AS first_invoice_date,
    MAX(invoice_date) AS last_invoice_date
  FROM current_invoices
  GROUP BY policy_id
),

payment_agg AS (
  SELECT
    policy_id,
    COUNT(*) AS total_payments,
    SUM(payment_amount) AS total_payment_amount,
    MAX(payment_date) AS last_payment_date
  FROM current_payments
  GROUP BY policy_id
)

SELECT
  i.policy_id,
  i.total_invoices,
  i.total_invoiced_amount,
  i.total_paid_amount,
  i.total_outstanding,
  i.overdue_count,
  i.arrears_count,
  i.first_invoice_date,
  i.last_invoice_date,
  COALESCE(p.total_payments, 0) AS total_payments,
  COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
  p.last_payment_date,
  CASE
    WHEN i.total_invoiced_amount > 0
    THEN ROUND(i.total_paid_amount / i.total_invoiced_amount * 100, 2)
    ELSE 0
  END AS payment_rate_pct
FROM invoice_agg i
LEFT JOIN payment_agg p ON i.policy_id = p.policy_id
