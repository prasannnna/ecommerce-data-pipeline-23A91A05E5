from sqlalchemy import text

def test_no_orphan_transactions(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM production.transactions t
            LEFT JOIN production.customers c
            ON t.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)).scalar()

    assert result == 0
