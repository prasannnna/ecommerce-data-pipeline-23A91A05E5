-- ==============================
-- COMPLETENESS CHECKS
-- ==============================

-- NULLs in mandatory product fields
SELECT COUNT(*) AS null_count
FROM production.products
WHERE product_name IS NULL
   OR category IS NULL
   OR price IS NULL;

-- Empty strings in text fields
SELECT COUNT(*) AS empty_strings
FROM production.customers
WHERE TRIM(email) = '';

-- Transactions without items
SELECT COUNT(*) AS transactions_without_items
FROM production.transactions t
LEFT JOIN production.transaction_items ti
  ON t.transaction_id = ti.transaction_id
WHERE ti.transaction_id IS NULL;


-- ==============================
-- UNIQUENESS CHECKS
-- ==============================

-- Duplicate customer IDs
SELECT customer_id, COUNT(*) 
FROM production.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Duplicate emails
SELECT email, COUNT(*)
FROM production.customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Duplicate transactions
SELECT customer_id, transaction_date, total_amount, COUNT(*)
FROM production.transactions
GROUP BY customer_id, transaction_date, total_amount
HAVING COUNT(*) > 1;


-- ==============================
-- VALIDITY & RANGE CHECKS
-- ==============================

-- Invalid price / quantity
SELECT COUNT(*) 
FROM production.products
WHERE price <= 0;

SELECT COUNT(*)
FROM production.transaction_items
WHERE quantity <= 0
   OR discount_percentage < 0
   OR discount_percentage > 100;


-- ==============================
-- CONSISTENCY CHECKS
-- ==============================

-- Line total mismatch
SELECT COUNT(*)
FROM production.transaction_items
WHERE ABS(
    line_total - (quantity * unit_price * (1 - discount_percentage/100))
) > 0.01;

-- Transaction total mismatch
SELECT COUNT(*)
FROM production.transactions t
JOIN production.transaction_items ti
  ON t.transaction_id = ti.transaction_id
GROUP BY t.transaction_id, t.total_amount
HAVING ABS(t.total_amount - SUM(ti.line_total)) > 0.01;


-- ==============================
-- REFERENTIAL INTEGRITY
-- ==============================

-- Orphan transactions
SELECT COUNT(*)
FROM production.transactions t
LEFT JOIN production.customers c
  ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction)
SELECT COUNT(*)
FROM production.transaction_items ti
LEFT JOIN production.transactions t
  ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product)
SELECT COUNT(*)
FROM production.transaction_items ti
LEFT JOIN production.products p
  ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;




-- Future transactions
SELECT COUNT(*)
FROM production.transactions
WHERE transaction_date > CURRENT_DATE;


SELECT COUNT(*)
FROM production.transactions t
JOIN production.customers c
  ON t.customer_id = c.customer_id
WHERE t.transaction_date < c.registration_date;
