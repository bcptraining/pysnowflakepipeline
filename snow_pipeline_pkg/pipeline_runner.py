import time
import sys
import os
import json
from snow_pipeline_pkg.ingest.avro_loader import load_avro_file
from snow_pipeline_pkg.transform.schema_mapper import (
    apply_column_mapping,
    drop_unmapped_columns,
)
from snow_pipeline_pkg.validate.quality_checks import (
    validate_schema_matches_table,
    validate_config_against_schema_ref,
)
from snow_pipeline_pkg.utils.log_setup import setup_logger
from snow_pipeline_pkg.utils.snowflake_session import managed_snowflake_session
from snow_pipeline_pkg.utils.config_loader import load_config
from snow_pipeline_pkg.writeback.stage_writer import (
    copy_to_table_semi_struct_data,
    collect_rejects,
)
from snow_pipeline_pkg.utils.validators import check_dependencies
import sys
from pathlib import Path

# --- Imports for normalize_column function ---
import os
import time
import json

# Import schema registry for optional copy_config vs schema validation
from snow_pipeline_pkg.config.schemas import schema_registry

import argparse
import re


def parse_args():
    # CLI argument parser for pipeline execution
    parser = argparse.ArgumentParser(
        description="Run Snowflake Avro ingestion pipeline."
    )
    parser.add_argument(
        "--connection-config",
        type=str,
        help="Path to Snowflake connection config",
        default=os.getenv(
            "SNOWFLAKE_CONFIG", "snow_pipeline_pkg/config/connection_details.json"
        ),
    )
    parser.add_argument(
        "--copy-config",
        type=str,
        help="Path to config describing COPY INTO statement",
        default=os.getenv(
            "COPY_CONFIG",
            "snow_pipeline_pkg/config/copy_to_snowstg_avro_emp_details_avro_cls.json",
        ),
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose debug logging"
    )
    return parser.parse_args()


# ---------- Utility Functions ----------


def safe_log_message(msg: str) -> str:
    # Replace non-ASCII characters with '?'
    return re.sub(r"[^\x00-\x7F]+", "?", msg)


def confirmation_msg(config, copy_result, rejects_df, result_df, log):
    # Summary of pipeline ingestion results
    db = config.get("database_name", "<db>")
    schema = config.get("schema_name", "<schema>")
    target_table = config.get("target_table", "<target>")
    rejects_table = config.get("reject_table", "<rejects>")

    target_fqn = f"{db}.{schema}.{target_table}"
    rejects_fqn = f"{db}.{schema}.{rejects_table}"

    cnt_files = result_df.count()
    cnt_rejected = rejects_df.count()
    cnt_inserted = sum(row.rows_loaded for row in copy_result)

    log.info(f"📂 {cnt_files} files loaded to {target_fqn}")
    log.info(f"✅ {cnt_inserted} rows added to {target_fqn}")
    log.warning(f"🚫 {cnt_rejected} rows sent to {rejects_fqn}")
    return cnt_files, cnt_inserted, cnt_rejected


def validate_config_keys(config, keys, logger):
    # Ensure required keys are present in config
    for key in keys:
        if key not in config:
            logger.error(f"❌ Config missing required key: {key}")
            raise KeyError(f"Missing config key: {key}")


# ---------- Pipeline Entry Point ----------


def main(config_path_override=None):
    args = parse_args()

    # ---------- 1. Load COPY config ----------
    connection_config_path = args.connection_config
    copy_config_path = config_path_override or args.copy_config
    copy_config = load_config(copy_config_path)

    # ---------- 2. Logger Initialization ----------
    log_file = copy_config.get("log_file", "logs/pipeline_v3.log")
    try:
        log = setup_logger(
            log_to_file=True,
            log_filename=log_file,
            name="avro_pipeline",
            verbose=args.verbose,
        )
        log.info(f"✅ Log initialized with log_file: {log_file}")
        log.info(f"✅ Using copy config file: {copy_config_path}")
        log.info(f"✅ Using connection config file: {connection_config_path}")
    except Exception:
        raise RuntimeError("Logger failed to initialize.")

    # ---------- 3. Check Dependencies ----------
    check_dependencies(log=log)

    # ---------- 4. Validate copy config ----------
    source_location = copy_config.get("source_location")
    if not source_location:
        log.error("❌ source_location is missing from config.")
        raise ValueError("Missing Source_location in configuration.")

    allow_partial_schema = copy_config.get("allow_partial_schema", False)
    required_keys = ["database_name", "schema_name", "target_table"]
    validate_config_keys(copy_config, required_keys, log)

    mapping = copy_config.get("map_columns", {})
    if not isinstance(mapping, dict):
        raise ValueError("⚠️ Column mapping in config must be a dictionary.")
    log.debug(f"🔄 Column mapping defined: {mapping}")

    # ---------- 5. Start pipeline and ingest Avro ----------
    log.info(f"✅ Pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()

    try:
        # ---------- 6. Create Snowflake session ----------
        log.info(f"🔗 Connecting to Snowflake with config: {connection_config_path}")
        with managed_snowflake_session(
            config_path=connection_config_path, log=log
        ) as session:

            df = load_avro_file(session, copy_config, log)
            log.info(
                f"🔍 Loaded Avro data from {source_location} with columns: {df.columns}"
            )

            # ---------- 7. Apply column mapping ----------
            df = apply_column_mapping(df, mapping, log)
            log.info(f"🔄 Renamed columns based on mapping: {df.columns}")
            mapped_columns = list(mapping.values())
            df = drop_unmapped_columns(df, mapped_columns, log)

            # ---------- 8. Validate schema conformity ----------
            target_columns = copy_config.get("target_columns", [])
            missing, extras = validate_schema_matches_table(df, target_columns, log)
            if missing or extras:
                log.warning(f"⚠️ Missing expected columns: {missing}")
                log.warning(f"⚠️ Unused columns from source: {extras}")
                if not allow_partial_schema:
                    raise ValueError(
                        "❌ Schema mismatch between source DataFrame and target table."
                    )

            # ---------- 9. Validate against schema reference ----------
            schema_ref = copy_config.get("validate_target_columns_against_schema")
            if schema_ref:
                missing, extra = validate_config_against_schema_ref(
                    target_columns, schema_ref, log
                )
                if (missing or extra) and not allow_partial_schema:
                    log.error("❌ Schema mismatch between config and schema reference.")
                    log.warning(f"⚠️ Missing columns in config: {missing}")
                    log.warning(f"⚠️ Extra columns in config: {extra}")
                    raise ValueError(
                        "Schema mismatch detected. Check logs for details."
                    )

            # ---------- 10. Execute COPY INTO operation ----------
            copied_into_result, qid = copy_to_table_semi_struct_data(
                session, copy_config, df, log=log
            )
            log.info(f"🔍 Query ID: {qid}")

            # ---------- 11. Create ingestion result audit ----------
            copied_into_result_df = session.create_dataframe(copied_into_result)
            rejects_df = collect_rejects(session, qid, copy_config, log=log)
            end_time = time.time()
            log.info(
                f"⏱️ Pipeline completed in {round(end_time - start_time, 2)} seconds"
            )

            cnt_files, cnt_inserted, cnt_rejected = confirmation_msg(
                copy_config, copied_into_result, rejects_df, copied_into_result_df, log
            )
            log.info("✅ Pipeline execution completed successfully.")

            # ---------- 12. Prepare pipeline summary ----------
            summary = {
                "start_time": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(start_time)
                ),
                "end_time": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(end_time)
                ),
                "duration_sec": round(end_time - start_time, 2),
                "files_loaded": cnt_files,
                "rows_inserted": cnt_inserted,
                "rows_rejected": cnt_rejected,
                "query_id": qid,
            }

            # ---------- 13. Include reject sample in summary ----------
            try:
                sample = rejects_df.limit(5).to_pandas().to_dict(orient="records")
            except Exception as e:
                log.warning(f"⚠️ Could not extract reject sample: {e}")
                sample = ["Reject sample unavailable"]

            summary["rejects"] = {
                "count": cnt_rejected,
                "columns": rejects_df.columns,
                "sample": sample,
            }

            # ---------- 14. Save summary to dashboard ----------
            mkdir_path = Path("dashboard")
            mkdir_path.mkdir(parents=True, exist_ok=True)
            with open(mkdir_path / "pipeline_summary.json", "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            log.info(f"📊 Summary written to: {mkdir_path / 'pipeline_summary.json'}")

            # ---------- 15. Maintain pipeline run history ----------
            history_path = "dashboard/pipeline_run_history.json"
            run_record = {
                "start_time": summary["start_time"],
                "end_time": summary["end_time"],
                "duration_sec": summary["duration_sec"],
                "files_loaded": summary["files_loaded"],
                "rows_inserted": summary["rows_inserted"],
                "rows_rejected": summary["rows_rejected"],
                "query_id": summary["query_id"],
            }

            try:
                if os.path.exists(history_path):
                    with open(history_path, "r", encoding="utf-8") as f:
                        history = json.load(f)
                        if not isinstance(history, list):
                            history = []
                else:
                    history = []
            except Exception:
                history = []

            history.append(run_record)
            with open(history_path, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)

    except Exception as err:
        log.error(f"❌ Pipeline execution failed: {err}")
        raise


# ---------- Entry Point Guard ----------

if __name__ == "__main__":
    main()
