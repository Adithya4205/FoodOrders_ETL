"""Batch-load validated rows into the separate analytics database."""
from __future__ import annotations

import config
from db_utils import connect_database, ensure_database, quote_identifier, sql_scalar


def create_analytics_table() -> None:
    ensure_database(config.ANALYTICS_DATABASE)
    conn = connect_database(config.ANALYTICS_DATABASE)
    try:
        cursor = conn.cursor()
        source_types = {
            "Order ID": "BIGINT NOT NULL",
            "Customer ID": "VARCHAR(64) NULL",
            "Restaurant ID": "VARCHAR(64) NULL",
            "Order Date and Time": "DATETIME NULL",
            "Delivery Date and Time": "DATETIME NULL",
            "Order Value": "DECIMAL(12,2) NULL",
            "Delivery Fee": "DECIMAL(12,2) NULL",
            "Payment Method": "VARCHAR(100) NULL",
            "Discounts and Offers": "VARCHAR(255) NULL",
            "Commission Fee": "DECIMAL(12,2) NULL",
            "Payment Processing Fee": "DECIMAL(12,2) NULL",
            "Refunds/Chargebacks": "DECIMAL(12,2) NULL",
            "Weather Condition": "VARCHAR(100) NULL",
            "Order Channel": "VARCHAR(100) NULL",
            "Delivery_Time_Minutes": "DECIMAL(10,2) NULL",
            "Order_Date": "DATE NULL",
            "Order_Hour": "TINYINT UNSIGNED NULL",
            "Platform_Net_Revenue": "DECIMAL(12,2) NULL",
            "delivery_quality_flag": "VARCHAR(64) NOT NULL",
        }
        columns = [
            "`analytics_row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
            "`pipeline_run_id` CHAR(36) NOT NULL",
            "`source_row_number` INT UNSIGNED NOT NULL",
        ] + [f"{quote_identifier(name)} {sql_type}" for name, sql_type in source_types.items()]
        columns += [
            "PRIMARY KEY (`analytics_row_id`)",
            "UNIQUE KEY `uq_run_source_row` (`pipeline_run_id`, `source_row_number`)",
            "KEY `ix_run_id` (`pipeline_run_id`)",
        ]
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {quote_identifier(config.ANALYTICS_TABLE)} (\n  "
            + ",\n  ".join(columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_transformed_batches(cleaned, run_id: str, batch_size=config.BATCH_SIZE) -> dict:
    create_analytics_table()
    conn = connect_database(config.ANALYTICS_DATABASE)
    cursor = conn.cursor()
    data_columns = config.CLEANED_COLUMNS + ["delivery_quality_flag"]
    insert_columns = ["pipeline_run_id", "source_row_number"] + data_columns
    sql = (
        f"INSERT INTO {quote_identifier(config.ANALYTICS_TABLE)} "
        f"({', '.join(quote_identifier(c) for c in insert_columns)}) "
        f"VALUES ({', '.join(['%s'] * len(insert_columns))})"
    )
    total_loaded = 0
    successful_batches = 0
    try:
        for start in range(0, len(cleaned), batch_size):
            part = cleaned.iloc[start:start + batch_size]
            values = []
            for _, row in part.iterrows():
                values.append((run_id, int(row["source_row_number"])) + tuple(
                    sql_scalar(row[column]) for column in data_columns
                ))
            batch_number = start // batch_size + 1
            try:
                cursor.executemany(sql, values)
                conn.commit()
                total_loaded += len(values)
                successful_batches += 1
                print(f"Analytics load batch {batch_number}: committed {len(values)} records")
            except Exception as exc:
                conn.rollback()
                error = RuntimeError(
                    f"Analytics database insertion failed for batch {batch_number}; "
                    "the batch transaction was rolled back."
                )
                error.successful_batches = successful_batches
                error.failed_batches = 1
                error.records_loaded = total_loaded
                raise error from exc
        cursor.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(config.ANALYTICS_TABLE)} WHERE `pipeline_run_id` = %s",
            (run_id,),
        )
        verified_count = int(cursor.fetchone()[0])
        if verified_count != total_loaded:
            raise RuntimeError(
                f"Analytics table count mismatch for run {run_id}: loaded {total_loaded}, verified {verified_count}."
            )
        return {"records_loaded": total_loaded, "successful_batches": successful_batches,
                "verified_records": verified_count}
    finally:
        cursor.close()
        conn.close()
