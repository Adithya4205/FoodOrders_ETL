"""Run the full extract-load-retrieve-transform-validate-load workflow."""
from __future__ import annotations

import json
import importlib.util
import socket
import sys
import uuid
from datetime import datetime, timezone

import config
from extract import iter_csv_batches
from load_raw import load_raw_batches
from retrieve_raw import retrieve_raw_batches
from transform import transform_batches
from validate import validate_transformed, write_csv_report, write_json_report
from load_transformed import load_transformed_batches


def run_pipeline():
    run_id = str(uuid.uuid4())
    report = {
        "report_type": "pipeline_execution",
        "pipeline_run_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(config.DATA_PATH),
        "batch_size": config.BATCH_SIZE,
        "total_records_extracted_from_csv": 0,
        "total_batches_processed": 0,
        "total_raw_records_loaded_into_mysql": None,
        "total_raw_records_retrieved_from_mysql": None,
        "total_valid_records_transformed": None,
        "total_rejected_records": None,
        "total_transformed_records_loaded_to_analytics_database": None,
        "successful_batches": 0,
        "failed_batches": 0,
        "final_validation_status": "NOT_RUN",
        "database_pipeline_status": "IN_PROGRESS",
        "stage_errors": [],
    }
    try:
        source_batches = list(iter_csv_batches(config.DATA_PATH, config.BATCH_SIZE, display=True))
        report["total_records_extracted_from_csv"] = sum(len(batch) for _, batch in source_batches)
        report["total_batches_processed"] = len(source_batches)
        # Calculate an offline preflight from the extracted data so data quality can
        # be reported even when MySQL is not configured on the execution machine.
        preflight_cleaned, preflight_rejected, preflight_stats = transform_batches(source_batches)
        preflight = validate_transformed(
            preflight_cleaned, preflight_rejected, preflight_stats,
            input_rows=report["total_records_extracted_from_csv"],
        )
        report["local_preflight_valid_records"] = len(preflight_cleaned)
        report["local_preflight_rejected_records"] = len(preflight_rejected)
        report["local_preflight_validation_status"] = preflight["final_validation_status"]
        report["local_preflight_validation_report"] = preflight
        config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
        write_json_report(preflight, config.REPORT_DIR / "validation_report.json")
        write_csv_report(preflight, config.REPORT_DIR / "validation_report.csv")
        try:
            driver_available = importlib.util.find_spec("mysql.connector") is not None
        except ModuleNotFoundError:
            driver_available = False
        try:
            with socket.create_connection((config.MYSQL_HOST, config.MYSQL_PORT), timeout=2):
                server_reachable = True
        except OSError:
            server_reachable = False
        report["database_prerequisites"] = {
            "mysql_server_reachable": server_reachable,
            "mysql_python_driver_available": driver_available,
            "host": config.MYSQL_HOST,
            "port": config.MYSQL_PORT,
        }
        if not server_reachable:
            raise RuntimeError(
                f"MySQL server is not reachable at {config.MYSQL_HOST}:{config.MYSQL_PORT}. "
                "The local data preflight completed, but no database stage was run."
            )
        if not driver_available:
            raise RuntimeError(
                "mysql-connector-python is not installed. Install requirements.txt before the database stages."
            )
        try:
            raw_result = load_raw_batches(iter(source_batches), run_id)
        except Exception as exc:
            report["total_raw_records_loaded_into_mysql"] = int(getattr(exc, "records_loaded", 0))
            report["successful_raw_batches"] = int(getattr(exc, "successful_batches", 0))
            report["failed_raw_batches"] = int(getattr(exc, "failed_batches", 0))
            report["stage_errors"].append({"stage": "raw_load", "error": f"{type(exc).__name__}: {exc}"})
            raise
        report["total_raw_records_loaded_into_mysql"] = raw_result["verified_records"]
        report["successful_raw_batches"] = raw_result["successful_batches"]
        report["successful_batches"] += raw_result["successful_batches"]

        retrieved = list(retrieve_raw_batches(run_id, config.BATCH_SIZE, display=True))
        report["total_raw_records_retrieved_from_mysql"] = sum(len(batch) for _, batch in retrieved)
        cleaned, rejected, transform_stats = transform_batches(retrieved)
        report["total_valid_records_transformed"] = len(cleaned)
        report["total_rejected_records"] = len(rejected)
        validation = validate_transformed(
            cleaned, rejected, transform_stats,
            input_rows=report["total_raw_records_retrieved_from_mysql"],
        )
        report["validation_checks"] = validation["checks"]
        report["final_validation_status"] = validation["final_validation_status"]
        report["local_validation_report"] = validation
        if validation["final_validation_status"] != "PASS":
            raise RuntimeError("Transformed data failed validation; analytics loading was stopped.")

        cleaned.insert(0, "pipeline_run_id", run_id)
        try:
            analytics_result = load_transformed_batches(cleaned.drop(columns=["pipeline_run_id"]), run_id)
        except Exception as exc:
            report["total_transformed_records_loaded_to_analytics_database"] = int(getattr(exc, "records_loaded", 0))
            report["successful_analytics_batches"] = int(getattr(exc, "successful_batches", 0))
            raise
        report["total_transformed_records_loaded_to_analytics_database"] = analytics_result["verified_records"]
        report["successful_analytics_batches"] = analytics_result["successful_batches"]
        report["successful_batches"] += analytics_result["successful_batches"]
        report["final_validation_status"] = "PASS"
        report["database_pipeline_status"] = "PASS"
        report["validation_report"] = validation
        config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
        if not rejected.empty:
            rejected.to_csv(config.REPORT_DIR / f"rejected_records_{run_id}.csv", index=False)
        write_json_report(validation, config.REPORT_DIR / f"validation_report_{run_id}.json")
        write_json_report(validation, config.REPORT_DIR / "validation_report.json")
        write_csv_report(validation, config.REPORT_DIR / "validation_report.csv")
        print("Pipeline completed successfully.")
    except Exception as exc:
        report["successful_batches"] += int(getattr(exc, "successful_batches", 0))
        report["failed_batches"] += int(getattr(exc, "failed_batches", 0))
        if not report["stage_errors"]:
            report["stage_errors"].append({"stage": "pipeline", "error": f"{type(exc).__name__}: {exc}"})
        prerequisites = report.get("database_prerequisites", {})
        if prerequisites and not (prerequisites.get("mysql_server_reachable") and
                                  prerequisites.get("mysql_python_driver_available")):
            report["database_pipeline_status"] = "NOT_RUN_PREREQUISITE_MISSING"
        else:
            report["database_pipeline_status"] = "FAILED"
        if report["final_validation_status"] == "NOT_RUN":
            report["final_validation_status"] = "LOCAL_PREFLIGHT_PASS; MYSQL_PIPELINE_NOT_RUN"
        print(f"Pipeline stopped: {type(exc).__name__}: {exc}")
    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = config.REPORT_DIR / "pipeline_execution_report.json"
        path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Execution report written to {path}")
        print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    result = run_pipeline()
    sys.exit(0 if result["database_pipeline_status"] == "PASS" else 1)
