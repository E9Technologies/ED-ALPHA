from __future__ import annotations

import os
from typing import Dict

from dotenv import load_dotenv


def load_configuration() -> Dict[str, Dict[str, str]]:
    """
    Load application configuration from environment variables (with .env support).
    Returns a dictionary containing the user's email and PostgreSQL connection settings.
    """
    load_dotenv()

    user_email = os.getenv("USER_EMAIL")
    if not user_email:
        raise ValueError("USER_EMAIL is required. Set it in your environment or .env file.")

    database_config = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
    }

    missing_db_fields = [
        key for key, value in database_config.items() if key not in {"host", "port"} and not value
    ]
    if missing_db_fields:
        raise ValueError(
            f"Missing required database configuration values: {', '.join(sorted(missing_db_fields))}"
        )

    database_config["port"] = int(database_config["port"])

    return {
        "user_email": user_email,
        "database_config": database_config,
    }


def build_user_agent(email: str) -> str:
    return f"Ed-Alpha/0.1 ({email})"
