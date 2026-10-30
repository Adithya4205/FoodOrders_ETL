"""Run the local data checks without requiring a running MySQL server."""
from __future__ import annotations

from extract import iter_csv_batches
from transform import transform_batches
from validate import validate_transformed, write_csv_report, write_json_report
import config


def main():
    batches = list(iter_csv_batches(config.DATA_PATH, config.BATCH_SIZE, display=True))
    extracted = sum(len(batch) for _, batch in batches)
    cleaned, rejected, stats = transform_batches(batches)
    report = validate_transformed(cleaned, rejected, stats, input_rows=extracted)
    report["database_pipeline_status"] = "NOT_RUN_MYSQL_SERVER_REQUIRED"
    config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    write_json_report(report, config.REPORT_DIR / "validation_report.json")
    write_csv_report(report, config.REPORT_DIR / "validation_report.csv")
    if not rejected.empty:
        rejected.to_csv(config.REPORT_DIR / "rejected_records.csv", index=False)
    print(report)
    return report


if __name__ == "__main__":
    main()
