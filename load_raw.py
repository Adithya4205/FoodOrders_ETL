"""Append the unmodified source records to the raw MySQL database."""
from __future__ import annotations

import pandas as pd

import config
from db_utils import connect_database, ensure_database, quote_identifier


def create_raw_table() -> None:
    ensure_database(config.RAW_DATABASE)
    conn = connect_database(config.RAW_DATABASE)
    try:
        cursor = conn.cursor()
        source_ddl = ",\n".join(f"{quote_identifier(column)} LONGTEXT NULL" for column in config.SOURCE_COLUMNS)
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {quote_identifier(config.RAW_TABLE)} (\n"
            "  `raw_row_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            "  `pipeline_run_id` CHAR(36) NOT NULL,\n"
            "  `source_row_number` INT UNSIGNED NOT NULL,\n"
            f"  {source_ddl},\n"
            "  PRIMARY KEY (`raw_row_id`),\n"
            "  UNIQUE KEY `uq_run_source_row` (`pipeline_run_id`, `source_row_number`),\n"
            "  KEY `ix_run_id` (`pipeline_run_id`)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_raw_batches(batches, run_id: str) -> dict:
    """Insert each extraction batch transactionally; never truncate earlier batches."""
    create_raw_table()
    conn = connect_database(config.RAW_DATABASE)
    cursor = conn.cursor()
    columns = ["pipeline_run_id", "source_row_number"] + config.SOURCE_COLUMNS
    names = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {quote_identifier(config.RAW_TABLE)} ({names}) VALUES ({placeholders})"
    total_loaded = 0
    successful_batches = 0
    try:
        for batch_number, batch in batches:
            rows = []
            for row in batch.itertuples(index=False, name=None):
                source_row_number = int(row[0])
                source_values = row[1:]
                clean_values = tuple(None if value == "" else str(value) for value in source_values)
                rows.append((run_id, source_row_number, *clean_values))
            try:
                cursor.executemany(sql, rows)
                conn.commit()
                total_loaded += len(rows)
                successful_batches += 1
                print(f"Raw load batch {batch_number}: committed {len(rows)} records")
            except Exception as exc:
                conn.rollback()
                error = RuntimeError(
                    f"Raw database insertion failed for batch {batch_number}; "
                    "the batch transaction was rolled back."
                )
                error.successful_batches = successful_batches
                error.failed_batches = 1
                error.records_loaded = total_loaded
                raise error from exc
        cursor.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(config.RAW_TABLE)} WHERE `pipeline_run_id` = %s",
            (run_id,),
        )
        verified_count = int(cursor.fetchone()[0])
        if verified_count != total_loaded:
            raise RuntimeError(
                f"Raw table count mismatch for run {run_id}: loaded {total_loaded}, verified {verified_count}."
            )
        return {"records_loaded": total_loaded, "successful_batches": successful_batches,
                "verified_records": verified_count}
    finally:
        cursor.close()
        conn.close()
