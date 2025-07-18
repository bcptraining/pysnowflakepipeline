from ingest.avro_loader import load_avro_file

from transform.schema_mapper import apply_column_mapping, drop_unmapped_columns

from validate.quality_checks import validate_schema_matches_table

from config.schemas import emp_details_avro_cls

from snow_pipeline_pkg.utils.log_setup import setup_logger

from snowflake.snowpark.types import StructType, StructField, StringType, LongType



# from snow_pipeline_pkg.utils.connection_loader import load_connection_config

from snow_pipeline_pkg.utils.snowflake_session import managed_snowflake_session

from snow_pipeline_pkg.utils.config_loader import load_config

from snow_pipeline_pkg.writeback.stage_writer import (

    copy_to_table_semi_struct_data,

    collect_rejects,

)

from snowflake.snowpark.types import StructType, StructField, StringType

from snow_pipeline_pkg.utils.validators import check_dependencies



# from transform.schema_mapper import map_columns

from pathlib import Path



# --- Imports for normalize_column function ---

from snowflake.snowpark.functions import col



# ---------- Utility Functions ----------





def confirmation_msg(config, copy_result, rejects_df, result_df, log):

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





# def normalize_column(c):

#     cleaned = c.strip('"')

#     return col(f'"{cleaned}"').alias(cleaned.upper())





def validate_config_keys(config, keys, logger):

    for key in keys:

        if key not in config:

            logger.error(f"❌ Config missing required key: {key}")

            raise KeyError(f"Missing config key: {key}")





# def load_config(config_path):

#     if not os.path.isfile(config_path):

#         raise FileNotFoundError(f"❌ File not found: {config_path}")

#     with open(config_path, "r", encoding="utf-8") as f:

#         content = f.read()

#         if not content.strip():

#             raise ValueError("⚠️ JSON config file is empty.")

#         return json.loads(content)



# def apply_column_mapping(df, mapping, log):

#     source_cols = set(df.columns)

#     for raw, mapped in mapping.items():

#         src = raw.strip('"').upper()

#         tgt = mapped.strip('"').upper()

#         if src in source_cols:

#             df = df.with_column_renamed(src, tgt)

#             log.info(f"🔄 Renamed '{src}' → '{tgt}'")

#         else:

#             log.warning(f"⚠️ Column '{src}' not found — skipped")

#     return df





#  --------- Import schemas ----------

# src_stg_schema.emp_details_avro_cls = src_stg_schema.int_emp_details_avro

expected_columns = [field.name for field in emp_details_avro_cls.fields]



# ---------- Logger Initialization ----------

# first we need to get log_file name from the config file

config_path = Path(

    "snow_pipeline_pkg/config/copy_to_snowstg_avro_emp_details_avro_cls.json"

)

copy_config = load_config(config_path)

log_file = copy_config.get(

    "log_file", "logs/pipeline_v3.log"

)  # set log_file from config or default

log = None

try:

    log = setup_logger(

        log_to_file=True,

        log_filename=log_file,  # Use the log_file from config

        name="AVRO_setup_logger",

    )

except Exception as _:

    raise RuntimeError("Logger failed to initialize.")



log.debug(f"✅ Log initialized with log_file: {log_file}")

start_time = time.time()



# ------------ Check Dependencies ----------

check_dependencies(log=log)



#  --------- Load copy configuration from default (describes copy) ----------

source_location = copy_config.get("source_location", None)



if not source_location:

    log.error("❌ source_location is missing from config.")

    raise ValueError("Missing Source_location in configuration.")

allow_partial_schema = copy_config.get("allow_partial_schema", False)

required_keys = [

    "database_name",

    "schema_name",

    "target_table",

]  # Ensure these keys are present in the config

validate_config_keys(copy_config, required_keys, log)

log.debug(f"✅ copy_config loaded with required keys present: {log_file}")



# ---------- PIPELINE EXECUTION ----------

log.info(f"✅ Pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")

start_time = time.time()



#  --------- 1. Determine path to Snowpark connection config ----------

# 3 ways the path can be determined: CLI arg, environment variable or default

config_override = (

    sys.argv[1] if len(sys.argv) > 1 else None

)  # if path is passed as CLI arg

# resolved_path = config_override or os.getenv(

#     "SNOWFLAKE_CONFIG", "./config/connection_details.json"

# )

resolved_path = config_override or os.getenv(

    "SNOWFLAKE_CONFIG", "snow_pipeline_pkg/config/connection_details.json"

)





log.info(f"📁 Connection config file determined to be: {resolved_path}")



#  --------- 2. Create Snowpark session and run the pipeline ----------



try:

    with managed_snowflake_session(config_path=resolved_path, log=log) as session:



        # # Inspect columns from raw Avro source file

        # if not source_location:

        #     log.error("❌ source_location is missing from config.")

        #     raise ValueError("Missing Source_location in configuration.")



        # try:

        #     df = session.read.avro(source_location)

        # except Exception as avro_err:

        #     log.error(f"❌ Failed to read Avro file: {avro_err}")

        #     raise



        # log.info(f"🔍 Loaded Avro data from {source_location} with columns: {df.columns}")



        if log is None:

            raise ValueError(

                "❌ Logger is not initialized. Make sure to setup logger prior to running the pipeline."

            )



        # 🛠️ Rename columns based on mapping defined in the config file



        mapping = copy_config.get("map_columns", {})

        if not isinstance(mapping, dict):

            raise ValueError("⚠️ Column mapping in config must be a dictionary.")

        else:

            log.debug(f"🔄 Column mapping defined: {mapping}")



        # Conform columns for comparison -- '"registration_dttm"' --> 'REGISTRATION_DTTM'

        # df = df.select([normalize_column(c) for c in df.columns])



        df = load_avro_file(session, copy_config, log)

        log.info(

            f"🔍 Loaded Avro data from {source_location} with columns: {df.columns}"

        )



        # Rename columns based on mapping keys — now aligned with normalized casing

        df = apply_column_mapping(df, mapping, log)

        log.info(f"🔄 Renamed columns based on mapping: {df.columns}")



        # Drop unmapped columns

        mapped_columns = list(mapping.values())

        log.debug(

            f"🧪 ---------------------DataFrame columns before drop: {df.columns}"

        )



        df = drop_unmapped_columns(df, mapped_columns, log)



        # Validate schema against target table

        log.info(f"Conformed and renamed (mapped) source columns: {df.columns}")

        log.info(f"Target columns: {copy_config['target_columns']}")

        missing, extras = validate_schema_matches_table(

            df, copy_config["target_columns"], log

        )

        if missing or extras:

            log.warning(f"⚠️ Missing expected columns: {missing}")

            log.warning(f"⚠️ Unused columns from source: {extras}")

            if not allow_partial_schema:

                log.error(

                    "❌ Schema mismatch between source DataFrame and target table definition."

                )

                raise ValueError("Schema mismatch detected. Check logs for details.")

        else:

            log.info("✅ All schema columns matched.")



        # Run pipeline

        copied_into_result, qid = copy_to_table_semi_struct_data(

            session,

            copy_config,

            # emp_details_avro_cls.schema if hasattr(emp_details_avro_cls
'schema') else None,

            df,  # This is the cleaned and validated data frame

            log=log,  # ← This must be explicitly passed as a keyword argument

        )



        log.info(f"🔍 Query ID: {qid}")

        copied_into_result_df = session.create_dataframe(copied_into_result)

        rejects_df = collect_rejects(session, qid, copy_config, log=log)



        end_time = time.time()

        log.info(f"⏱️ Pipeline completed in {round(end_time - start_time, 2)} seconds")

        cnt_files, cnt_inserted, cnt_rejected = confirmation_msg(

            copy_config, copied_into_result, rejects_df, copied_into_result_df, log

        )



        log.info("✅ Pipeline execution completed successfully.")



        #  Output a summary record to a JSON file to be surfaced in a streamlit dashboard

        files_loaded = copied_into_result_df.count()

        rows_rejected = rejects_df.count()

        summary = {

            "start_time": time.strftime(

                "%Y-%m-%d %H:%M:%S", time.localtime(start_time)

            ),

            "end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)),

            "duration_sec": round(end_time - start_time, 2),

            "files_loaded": cnt_files,

            "rows_inserted": cnt_inserted,

            "rows_rejected": cnt_rejected,

            "query_id": qid,

        }

        # Summarize the rejects

        files_loaded = copied_into_result_df.count()

        rows_rejected = rejects_df.count()

        summary = {

            "start_time": time.strftime(

                "%Y-%m-%d %H:%M:%S", time.localtime(start_time)

            ),

            "end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)),

            "duration_sec": round(end_time - start_time, 2),

            "files_loaded": cnt_files,

            "rows_inserted": cnt_inserted,

            "rows_rejected": cnt_rejected,

            "query_id": qid,

        }



        # Handle reject sample safely in case pandas isn't detected

        sample = []

        try:

            sample = rejects_df.limit(5).to_pandas().to_dict(orient="records")

        except Exception as _:

            log.warning(f"⚠️ Could not extract reject sample: {e}")

            sample = ["Reject sample unavailable"]



        reject_summary = {

            "count": cnt_rejected,

            "columns": rejects_df.columns,

            "sample": sample,

        }

        summary["rejects"] = reject_summary



        mkdir_path = Path("dashboard")

        mkdir_path.mkdir(parents=True, exist_ok=True)

        with open(mkdir_path / "pipeline_summary.json", "w", encoding="utf-8") as f:

            json.dump(summary, f, indent=2)

        log.info(f"📊 Summary written to: {mkdir_path / 'pipeline_summary.json'}")



        # # Summarize the rejects

        # reject_summary = {

        #     "count": cnt_rejected,

        #     "columns": rejects_df.columns,

        #     "sample": rejects_df.limit(5).to_pandas().to_dict(orient="records"),

        # }

        # summary["rejects"] = reject_summary

        # mkdir_path = Path("dashboard")

        # mkdir_path.mkdir(parents=True, exist_ok=True)

        # with open(mkdir_path / "pipeline_summary.json", "w", encoding="utf-8") as f:

        #     json.dump(summary, f, indent=2)

        # log.info(f"📊 Summary written to: {mkdir_path / 'pipeline_summary.json'}")

except Exception as _rr:

    log.error(f"❌ Pipeline execution failed: {err}")

    raise





# log = setup_logger(log_to_file=True, log_filename="logs/pipeline.log")

# # log = logging.getLogger(__name__)





# # log = logging.getLogger("session_logger")

# expected_columns = [field.name for field in emp_details_avro_cls.fields]



# df = load_avro_file(session, config_snow_copy, log)

# # # Normalize column names (strip quotes, uppercase)

# # df = apply_column_mapping(df, src_stg_schema.emp_details_avro_cls.column_mapping
log)



# df = apply_column_mapping(df, config["map_columns"], log)

# df = drop_unmapped_columns(df, config["map_columns"], log)



# missing, extras = validate_schema_matches_table(df, config["target_columns"], log)

