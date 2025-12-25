from sqlalchemy import text

def test_staging_tables_exist(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'staging'
        """)).fetchall()

    tables = {row[0] for row in result}

    expected_tables = {
        "customers",
        "products",
        "transactions",
        "transaction_items"
    }

    assert expected_tables.issubset(tables)
