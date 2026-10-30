"""Configuration for the FoodOrders ETL project and Task 9 pipeline."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
DATA_PATH = Path(os.getenv("FOOD_ORDERS_CSV", str(PROJECT_DIR / "food_orders.csv"))).expanduser()
REPORT_DIR = Path(os.getenv("FOOD_ORDERS_REPORT_DIR", str(PROJECT_DIR / "Task_9_Pipeline" / "reports"))).expanduser()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
BATCH_SIZE = int(os.getenv("FOOD_ORDERS_BATCH_SIZE", "100"))

def _setting(primary: str, reference_name: str, default: str) -> str:
    value = os.getenv(primary)
    if value is None:
        value = os.getenv(reference_name)
    return default if value is None else value

# Accept both this project's MYSQL_* settings and the reference project's DB_* names.
MYSQL_HOST = _setting("MYSQL_HOST", "DB_HOST", "127.0.0.1")
MYSQL_PORT = int(_setting("MYSQL_PORT", "DB_PORT", "3306"))
MYSQL_USER = _setting("MYSQL_USER", "DB_USER", "root")
MYSQL_PASSWORD = _setting("MYSQL_PASSWORD", "DB_PASSWORD", "")
RAW_DATABASE = "food_orders_raw_db"
RAW_TABLE = "raw_food_orders"
ANALYTICS_DATABASE = "food_orders_analytics_db"
ANALYTICS_TABLE = "cleaned_food_orders"

SOURCE_COLUMNS = [
    "Order ID", "Customer ID", "Restaurant ID", "Order Date and Time",
    "Delivery Date and Time", "Order Value", "Delivery Fee", "Payment Method",
    "Discounts and Offers", "Commission Fee", "Payment Processing Fee",
    "Refunds/Chargebacks", "Weather Condition", "Order Channel",
]
DATETIME_COLUMNS = ["Order Date and Time", "Delivery Date and Time"]
FINANCIAL_COLUMNS = [
    "Order Value", "Delivery Fee", "Commission Fee",
    "Payment Processing Fee", "Refunds/Chargebacks",
]
REVENUE_COMPONENTS = [
    "Commission Fee", "Delivery Fee", "Payment Processing Fee", "Refunds/Chargebacks",
]
CLEANED_COLUMNS = SOURCE_COLUMNS + [
    "Delivery_Time_Minutes", "Order_Date", "Order_Hour", "Platform_Net_Revenue",
]
