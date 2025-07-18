import sys
import os
import time
from pathlib import Path
from generic_code import code_library_cp
from generic_code.code_library_cp import (
    managed_snowflake_session,
    validate_schema_matches_table,
)
from log_utils import setup_logger
from schema import src_stg_schema
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import json

# ---------- Utility Functions ----------


def confirmation_msg(config, copy_result, rejects_df, result_df, log):
    db = config.get("Database_name", "<db>")
    schema = config.get("Schema_name", "<schema>")
    target_table = config.get("Target_table", "<target>")
    rejects_table = config.get("Reject_table", "<rejects>")

    target_fqn = f"{db}.{schema}.{target_table}"
    rejects_fqn = f"{db}.{schema}.{rejects_table}"

    cnt_files = result_df.count()
    cnt_rejected = rejects_df.count()
    cnt_inserted = sum(row.rows_loaded for row in copy_result)

    log.info(f"📂 {cnt_files} files loaded to {target_fqn}")
    log.info(f"✅ {cnt_inserted} rows added to {target_fqn}")
    log.warning(f"🚫 {cnt_rejected} rows sent to {rejects_fqn}")
    return cnt_files, cnt_inserted, cnt_rejected


def normalize_column(c):
    cleaned = c.strip('"')
    return col(f'"{cleaned}"').alias(cleaned.upper())


def load_config(config_path):
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"❌ File not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()
        if not content.strip():
            raise ValueError("⚠️ JSON config file is empty.")
        return json.loads(content)


def apply_column_mapping(df, mapping, log):
    source_cols = set(df.columns)
    for raw, mapped in mapping.items():
        src = raw.strip('"').upper()
        tgt = mapped.strip('"').upper()
        if src in source_cols:
            df = df.with_column_renamed(src, tgt)
            log.info(f"🔄 Renamed '{src}' → '{tgt}'")
        else:
            log.warning(f"⚠️ Column '{src}' not found — skipped")
    return df


def run_pipeline(config_path):
    # # Ensure the script is run in the correct environment
    # if not get_active_session():
    #     raise RuntimeError("❌ No active Snowflake session found. Please ensure you are connected to Snowflake.")

    #  --------- Import schemas ----------
    src_stg_schema.emp_details_avro_cls = src_stg_schema.int_emp_details_avro

    #  --------- Check for required packages ----------
    try:
        import snowflake.snowpark
        import snowflake.snowpark.functions as F
    except ImportError as e:
        log.error(f"❌ Required package not found: {e}")
        sys.exit(1)
    #  --------- Load configuration (describes copy) ----------
    # config_path  =  Path("config/copy_to_snowstg_avro_v1.json")  # Path to your config file
    config_snow_copy = load_config(config_path)
    source_location = config_snow_copy.get("source_location", None)
    log_file = config_snow_copy.get("log_file", "logs/pipeline_v3.log")
    allow_partial_schema = config_snow_copy.get("allow_partial_schema", False)
    required_keys = [
        "Database_name",
        "Schema_name",
        "Target_table",
    ]  # Ensure these keys are present in the config
    for key in required_keys:
        if key not in config_snow_copy:
            log.error(f"❌ Config missing required key: {key}")
            raise KeyError(f"Missing config key: {key}")

    # ---------- Logger Initialization ----------
    log = None
    try:
        log = setup_logger(
            log_to_file=True,
            log_filename=log_file,  # Use the log_file from config
            name="AVRO emp_load_to_stg_v3.py",
        )
    except Exception as e:
        raise RuntimeError("Logger failed to initialize.")

    # ---------- Pipeline Execution ----------
    log.info(f"✅ Pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()

    try:
        with managed_snowflake_session(log=log) as session:

            # Inspect columns from raw Avro source file
            if not source_location:
                log.error("❌ source_location is missing from config.")
                raise ValueError("Missing Source_location in configuration.")

            try:
                df = session.read.avro(source_location)
            except Exception as avro_err:
                log.error(f"❌ Failed to read Avro file: {avro_err}")
                raise

            log.info(
                f"🔍 Loaded Avro data from {source_location} with columns: {df.columns}"
            )

            # 🛠️ Rename columns based on mapping defined in the config file
            mapping = config_snow_copy.get("map_columns", {})
            if not isinstance(mapping, dict):
                raise ValueError("⚠️ Column mapping in config must be a dictionary.")

            # Conform columns for comparison -- '"registration_dttm"' --> 'REGISTRATION_DTTM'
            df = df.select([normalize_column(c) for c in df.columns])

            # Rename columns based on mapping keys — now aligned with normalized casing
            df = apply_column_mapping(df, mapping, log)

            # Validate schema against target table
            log.info(f"Conformed and renamed (mapped) source columns: {df.columns}")
            log.info(f"Target columns: {config_snow_copy['target_columns']}")
            missing, extras = validate_schema_matches_table(
                df, config_snow_copy["target_columns"]
            )
            if missing or extras:
                log.warning(f"⚠️ Missing expected columns: {missing}")
                log.warning(f"⚠️ Unused columns from source: {extras}")
                if allow_partial_schema == False:
                    log.error(
                        "❌ Schema mismatch between source DataFrame and target table definition."
                    )
                    raise ValueError(
                        "Schema mismatch detected. Check logs for details."
                    )
            else:
                log.info("✅ All schema columns matched.")

            # Run pipeline
            copied_into_result, qid = code_library_cp.copy_to_table_semi_struct_data(
                session, config_snow_copy, src_stg_schema.emp_details_avro_cls
            )

            log.info(f"🔍 Query ID: {qid}")
            copied_into_result_df = session.create_dataframe(copied_into_result)
            rejects_df = code_library_cp.collect_rejects(session, qid, config_snow_copy)

            end_time = time.time()
            log.info(
                f"⏱️ Pipeline completed in {round(end_time - start_time, 2)} seconds"
            )
            cnt_files, cnt_inserted, cnt_rejected = confirmation_msg(
                config_snow_copy,
                copied_into_result,
                rejects_df,
                copied_into_result_df,
                log,
            )

            log.info("✅ Pipeline execution completed successfully.")

            #  Output a summary record to a JSON file to be surfaced in a streamlit dashboard
            files_loaded = copied_into_result_df.count()
            rows_rejected = rejects_df.count()
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
            # Summarize the rejects
            reject_summary = {
                "count": cnt_rejected,
                "columns": rejects_df.columns,
                "sample": rejects_df.limit(5).to_pandas().to_dict(orient="records"),
            }
            summary["rejects"] = reject_summary
            with open("dashboard/pipeline_summary.json", "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
    except Exception as err:
        log.error(f"❌ Pipeline execution failed: {err}")
        raise
