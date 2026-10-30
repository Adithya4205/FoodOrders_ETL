"""Apply documented cleaning rules to data retrieved from the raw table."""
from __future__ import annotations

import pandas as pd

import config


def transform_batches(retrieved_batches, implausible_delivery_minutes=240):
    """Transform all retrieved batches together so cross-batch duplicates are caught.

    Exact duplicate source rows are rejected after global comparison. Distinct rows
    sharing an Order ID are retained and surfaced for investigation. Missing
    financial components remain missing; net revenue is computed only when all
    four required components are present.
    """
    frames = [frame.copy() for _, frame in retrieved_batches]
    if frames:
        raw = pd.concat(frames, ignore_index=True)
    else:
        raw = pd.DataFrame(columns=["source_row_number"] + config.SOURCE_COLUMNS)
    if raw.empty:
        return raw.copy(), raw.copy(), {"exact_duplicate_rows": 0, "invalid_timestamp_rows": 0,
                                      "negative_duration_rows": 0, "invalid_order_id_rows": 0,
                                      "repeated_order_ids_retained": 0}

    # Compare all original source fields across the entire run, not one batch at a time.
    duplicate_mask = raw.duplicated(subset=config.SOURCE_COLUMNS, keep="first")
    duplicate_rejects = raw.loc[duplicate_mask].copy()
    duplicate_rejects["rejection_reason"] = "exact_duplicate_source_row"
    unique = raw.loc[~duplicate_mask].copy()

    for column in config.DATETIME_COLUMNS:
        unique[column] = pd.to_datetime(unique[column], errors="coerce")
    parse_failures = unique[config.DATETIME_COLUMNS].isna().any(axis=1)

    for column in config.FINANCIAL_COLUMNS:
        unique[column] = pd.to_numeric(unique[column], errors="coerce")

    parsed_order_id = pd.to_numeric(unique["Order ID"], errors="coerce")
    invalid_order_id = parsed_order_id.isna()
    unique["Order ID"] = parsed_order_id.astype("Int64")
    duration = (unique["Delivery Date and Time"] - unique["Order Date and Time"]).dt.total_seconds() / 60
    negative_duration = duration.lt(0).fillna(False)
    reject_mask = parse_failures | invalid_order_id | negative_duration

    invalid_rejects = unique.loc[reject_mask].copy()
    reason_parts = []
    for index in invalid_rejects.index:
        reasons = []
        if bool(parse_failures.loc[index]):
            reasons.append("invalid_or_missing_timestamp")
        if bool(invalid_order_id.loc[index]):
            reasons.append("invalid_or_missing_order_id")
        if bool(negative_duration.loc[index]):
            reasons.append("negative_delivery_duration")
        reason_parts.append(";".join(reasons))
    invalid_rejects["rejection_reason"] = reason_parts
    reject_parts = [part for part in (duplicate_rejects, invalid_rejects) if not part.empty]
    rejected = (pd.concat(reject_parts, ignore_index=True, sort=False) if reject_parts
                else raw.iloc[0:0].copy().assign(rejection_reason=pd.Series(dtype="object")))

    cleaned = unique.loc[~reject_mask].copy()
    cleaned["Delivery_Time_Minutes"] = duration.loc[cleaned.index].astype("float64")
    cleaned["Order_Date"] = cleaned["Order Date and Time"].dt.date
    cleaned["Order_Hour"] = cleaned["Order Date and Time"].dt.hour.astype("Int64")

    for column in ["Payment Method", "Discounts and Offers", "Weather Condition", "Order Channel"]:
        cleaned[column] = cleaned[column].astype("string").str.strip().replace("", pd.NA)
    cleaned["Payment Method"] = cleaned["Payment Method"].fillna("Unknown")
    cleaned["Discounts and Offers"] = cleaned["Discounts and Offers"].fillna("Not Specified")

    required = config.REVENUE_COMPONENTS
    revenue_components_available = cleaned[required].notna().all(axis=1)
    revenue = (
        cleaned["Commission Fee"] + cleaned["Delivery Fee"]
        - cleaned["Payment Processing Fee"] - cleaned["Refunds/Chargebacks"]
    )
    cleaned["Platform_Net_Revenue"] = revenue.where(revenue_components_available)
    cleaned["delivery_quality_flag"] = "valid"
    cleaned.loc[cleaned["Delivery_Time_Minutes"] > implausible_delivery_minutes,
                "delivery_quality_flag"] = "review_implausibly_long"

    remaining_duplicate_ids = cleaned["Order ID"].duplicated(keep=False)
    repeated_id_count = int(cleaned.loc[remaining_duplicate_ids, "Order ID"].nunique())
    stats = {
        "exact_duplicate_rows": int(duplicate_mask.sum()),
        "invalid_timestamp_rows": int(parse_failures.sum()),
        "negative_duration_rows": int(negative_duration.sum()),
        "invalid_order_id_rows": int(invalid_order_id.sum()),
        "repeated_order_ids_retained": repeated_id_count,
        "implausibly_long_delivery_rows_flagged": int((cleaned["Delivery_Time_Minutes"] > implausible_delivery_minutes).sum()),
        "revenue_eligible_rows": int(revenue_components_available.sum()),
    }
    return cleaned.reset_index(drop=True), rejected, stats
