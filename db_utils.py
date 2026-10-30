"""Small MySQL helpers used by the batch pipeline."""
from __future__ import annotations

import config


def _connector():
    """Load the optional driver at connection time so reports can explain missing setup."""
    import mysql.connector
    return mysql.connector


def connect_server():
    """Connect to MySQL without selecting a database."""
    return _connector().connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        autocommit=False,
        connection_timeout=5,
    )


def connect_database(database: str):
    """Connect to a named database with transactions enabled."""
    return _connector().connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=database,
        autocommit=False,
        connection_timeout=5,
    )


def ensure_database(database: str) -> None:
    """Create a database if it does not already exist."""
    connector = _connector()
    conn = connect_server()
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        cursor.close()
    except connector.Error:
        conn.rollback()
        raise
    finally:
        conn.close()


def quote_identifier(value: str) -> str:
    """Quote a MySQL identifier, escaping embedded backticks."""
    return "`" + value.replace("`", "``") + "`"


def sql_scalar(value):
    """Convert pandas/numpy/date scalar values to mysql-connector values."""
    import math
    import numpy as np
    import pandas as pd
    from datetime import date, datetime
    from decimal import Decimal

    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if value is None:
        return None
    missing = pd.isna(value)
    if isinstance(missing, (bool, np.bool_)) and missing:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    return value
