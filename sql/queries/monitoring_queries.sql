-- Monitoring Query 1: Data Freshness
SELECT
    'staging' AS layer,
    MAX(loaded_at) AS last_updated
FROM staging.customers

UNION ALL

SELECT
    'warehouse' AS layer,
    MAX(created_at) AS last_updated
FROM warehouse.fact_sales;

-- Monitoring Query 2: Orphan Records
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
LEFT JOIN production.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Monitoring Query 3: Daily Volume Trend
SELECT
    d.full_date,
    COUNT(DISTINCT f.transaction_id) AS transactions
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date DESC
LIMIT 30;
