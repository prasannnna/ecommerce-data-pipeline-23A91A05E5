from sqlalchemy import text

def test_fact_sales_loaded(engine):
    with engine.connect() as conn:
        count = conn.execute(text(
            "SELECT COUNT(*) FROM warehouse.fact_sales"
        )).scalar()

    assert count > 0
