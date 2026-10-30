"""Read the source CSV as unchanged text in fixed-size batches."""
from __future__ import annotations

import pandas as pd
import config


def iter_csv_batches(path=config.DATA_PATH, batch_size=config.BATCH_SIZE, display=True):
    """Yield (batch_number, DataFrame) with a stable 1-based source row number.

    Empty CSV fields remain empty strings at extraction time. The raw loader maps
    those empty fields to SQL NULL; all other source strings remain unchanged.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    source_row_number = 0
    for batch_number, batch in enumerate(
        pd.read_csv(path, dtype="string", keep_default_na=False, chunksize=batch_size), start=1
    ):
        actual_columns = list(batch.columns)
        if actual_columns != config.SOURCE_COLUMNS:
            raise ValueError(
                "CSV header does not match the assignment schema. "
                f"Expected {config.SOURCE_COLUMNS}; received {actual_columns}."
            )
        batch = batch.copy()
        first_row_number = source_row_number + 1
        source_row_number += len(batch)
        batch.insert(0, "source_row_number", range(first_row_number, source_row_number + 1))
        if display:
            print(f"Extract batch {batch_number}: {len(batch)} records")
        yield batch_number, batch
