import os
import pytest
from sqlalchemy import create_engine
from dotenv import load_dotenv

@pytest.fixture(scope="session")
def engine():
    # Load environment variables for pytest
    load_dotenv()

    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_name = os.getenv("DB_NAME", "ecommerce_db")
    db_port = os.getenv("DB_PORT", "5432")

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    yield engine

    engine.dispose()
