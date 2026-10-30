"""Retrieve one pipeline run from the raw table in bounded batches."""
from __future__ import annotations

import pandas as pd

import config
from db_utils import connect_database, quote_identifier


def retrieve_raw_batches(run_id: str, batch_size=config.BATCH_SIZE, display=True):
    conn = connect_database(config.RAW_DATABASE)
    cursor = conn.cursor()
    columns = ["source_row_number"] + config.SOURCE_COLUMNS
    select_columns = ", ".join(quote_identifier(column) for column in columns)
    table = quote_identifier(config.RAW_TABLE)
    last_source_row = 0
    batch_number = 0
    try:
        while True:
            cursor.execute(
                f"SELECT {select_columns} FROM {table} "
                "WHERE `pipeline_run_id` = %s AND `source_row_number` > %s "
                "ORDER BY `source_row_number` LIMIT %s",
                (run_id, last_source_row, batch_size),
            )
            rows = cursor.fetchall()
            if not rows:
                break
            batch_number += 1
            frame = pd.DataFrame(rows, columns=columns)
            last_source_row = int(frame["source_row_number"].max())
            if display:
                print(f"Retrieve batch {batch_number}: {len(frame)} records")
            yield batch_number, frame
    finally:
        cursor.close()
        conn.close()
