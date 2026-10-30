"""Validation checks and JSON reporting for transformed food order data."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import config


def validate_transformed(cleaned: pd.DataFrame, rejected: pd.DataFrame, stats: dict,
                         input_rows: int | None = None) -> dict:
    expected = set(config.CLEANED_COLUMNS + ["source_row_number", "delivery_quality_flag"])
    actual = set(cleaned.columns)
    datetime_ok = all(pd.api.types.is_datetime64_any_dtype(cleaned[c]) for c in config.DATETIME_COLUMNS)
    numeric_columns = config.FINANCIAL_COLUMNS + ["Delivery_Time_Minutes", "Platform_Net_Revenue"]
    numeric_ok = all(pd.api.types.is_numeric_dtype(cleaned[c]) for c in numeric_columns)
    required = config.REVENUE_COMPONENTS
    revenue_mask = cleaned[required].notna().all(axis=1)
    expected_revenue = (cleaned["Commission Fee"] + cleaned["Delivery Fee"]
                        - cleaned["Payment Processing Fee"] - cleaned["Refunds/Chargebacks"])
    revenue_ok = bool((cleaned.loc[revenue_mask, "Platform_Net_Revenue"].sub(
        expected_revenue.loc[revenue_mask]).abs() < 1e-9).all()) and bool(
            cleaned.loc[~revenue_mask, "Platform_Net_Revenue"].isna().all())
    expected_duration = (cleaned["Delivery Date and Time"] - cleaned["Order Date and Time"]).dt.total_seconds() / 60
    duration_ok = bool((cleaned["Delivery_Time_Minutes"].sub(expected_duration).abs() < 1e-9).all())
    repeated_order_id_rows = int(cleaned["Order ID"].duplicated(keep=False).sum())
    checks = {
        "expected_columns_present": expected.issubset(actual),
        "datetime_types_valid": datetime_ok,
        "financial_and_derived_numeric_types_valid": numeric_ok,
        "no_exact_duplicate_source_rows_in_cleaned_data": not cleaned.duplicated(subset=config.SOURCE_COLUMNS).any(),
        "no_negative_delivery_duration_in_valid_data": not cleaned["Delivery_Time_Minutes"].lt(0).any(),
        "repeated_order_ids_checked_and_reported": "Order ID" in cleaned.columns,
        "delivery_duration_formula_valid": duration_ok,
        "revenue_formula_and_missingness_valid": revenue_ok,
        "rejected_records_have_reasons": bool(rejected.empty or ("rejection_reason" in rejected.columns and rejected["rejection_reason"].notna().all())),
        "row_accounting_balances": (input_rows is None or len(cleaned) + len(rejected) == input_rows),
    }
    missing_after = cleaned.isna().sum().astype(int).to_dict()
    result = {
        "report_type": "local_data_validation",
        "input_records": int(input_rows if input_rows is not None else len(cleaned) + len(rejected)),
        "transformed_valid_records": int(len(cleaned)),
        "rejected_records": int(len(rejected)),
        "rejection_reason_counts": (rejected["rejection_reason"].value_counts(dropna=False).astype(int).to_dict()
                                    if not rejected.empty and "rejection_reason" in rejected else {}),
        "exact_duplicate_rows_removed": int(stats.get("exact_duplicate_rows", 0)),
        "invalid_timestamp_rows_rejected": int(stats.get("invalid_timestamp_rows", 0)),
        "negative_duration_rows_rejected": int(stats.get("negative_duration_rows", 0)),
        "implausibly_long_delivery_rows_flagged_and_retained": int(stats.get("implausibly_long_delivery_rows_flagged", 0)),
        "repeated_order_ids_retained_for_review": int(stats.get("repeated_order_ids_retained", 0)),
        "repeated_order_id_rows_in_transformed_data": repeated_order_id_rows,
        "revenue_eligible_records": int(stats.get("revenue_eligible_rows", 0)),
        "missing_values_after_cleaning": missing_after,
        "checks": checks,
        "final_validation_status": "PASS" if all(checks.values()) else "FAIL",
        "database_pipeline_status": "NOT_RUN",
    }
    return result


def write_json_report(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, default=str)
        stream.write("\n")


def write_csv_report(report: dict, path: Path) -> None:
    """Write a readable, spreadsheet-friendly summary of validation results."""
    rows = []
    for key, value in report.items():
        if key == "checks":
            continue
        rows.append({
            "section": "summary",
            "metric": key,
            "value": json.dumps(value, sort_keys=True, default=str)
            if isinstance(value, (dict, list, tuple)) else value,
        })
    for key, value in report.get("checks", {}).items():
        rows.append({"section": "validation_check", "metric": key, "value": value})
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["section", "metric", "value"]).to_csv(path, index=False)
