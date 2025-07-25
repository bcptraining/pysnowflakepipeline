from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType

# from transform.schema_mapper import map_columns
import os
import logging
import time
from snow_pipeline_pkg.transform.schema_mapper import (
    apply_column_mapping,
    drop_unmapped_columns,
)
from snowflake.snowpark.types import StructType


def collect_rejects(session, qid, config_file, log):
    # (1) Handle empty or missing Query ID gracefully
    if not qid:
        log.warning("⚠️ No Query ID provided. Skipping reject collection.")
        empty_schema = StructType(
            [
                StructField("ROW_STATUS", StringType()),
                StructField("ERROR_MESSAGE", StringType()),
                StructField("RAW_ROW", StringType()),
            ]
        )
        return session.create_dataframe([], schema=empty_schema)

    # (2) Extract config values for database/table reference
    database_name = config_file.get("database_name")
    schema_name = config_file.get("schema_name")
    target_table = config_file.get("target_table")
    reject_table = config_file.get("reject_table")

    # (3) Construct Snowflake VALIDATE query for reject inspection
    sql = f"""SELECT * FROM TABLE(
        VALIDATE(
            {database_name}.{schema_name}.{target_table},
            JOB_ID => '{qid}'
        )
    )"""

    # (4) Execute query and write rejects to the configured reject table
    log.debug(f"🧪 Running reject collection query: {sql}")
    rejects = session.sql(sql)
    rejects.write.mode("append").save_as_table(reject_table)
    log.info(f"✅ Rejects written to table: {reject_table}")

    return rejects


# ---------- Copy to table for semi-structured data ----------


def copy_to_table_semi_struct_data(session, config_file, df, schema="NA", log=None):
    # (1) Read required config values
    database_name = config_file.get("database_name")
    schema_name = config_file.get("schema_name")
    target_table = config_file.get("target_table")
    target_columns = config_file.get("target_columns")
    on_error = config_file.get("on_error")
    # source_location = config_file.get("source_location")
    # transformations = config_file.get("transformations")
    # mapped_columns = config_file.get("map_columns")
    # source_type = config_file.get("source_file_type")

    # (2) Create a temporary stage for loading data
    session.sql("create or replace temp stage demo_db.public.mystage").collect()
    remote_file_path = "@demo_db.public.mystage/" + target_table + "/"

    # (3) Write DataFrame to internal stage as CSV
    rows_to_be_written_to_temporary_stage = df.count()
    copy_into_temp_stage_result = df.write.copy_into_location(
        remote_file_path,
        file_format_type="csv",
        format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},
        header=False,
        overwrite=True,
    )

    # (4) Evaluate success of unload operation
    for row in copy_into_temp_stage_result:
        rows_successfully_written_to_temporary_stage = row["rows_unloaded"]

    if (
        rows_successfully_written_to_temporary_stage
        != rows_to_be_written_to_temporary_stage
    ):
        log.error("❌ Error writing data to temporary stage. Aborting copy operation.")
        raise ValueError("Error writing data to temporary stage.")
    else:
        log.debug(
            f"✅ Successfully wrote {rows_successfully_written_to_temporary_stage} rows to stage: {remote_file_path}"
            + f" with columns {df.columns}"
        )

    # (5) Read staged data back into Snowpark DataFrame
    df = session.read.csv("'" + remote_file_path + "'")
    rows_read_from_temporary_stage = df.count()
    log.debug(
        f"📥 Read {rows_read_from_temporary_stage} rows from stage: {remote_file_path}"
        + f" with columns {df.columns}"
    )

    if rows_read_from_temporary_stage != rows_successfully_written_to_temporary_stage:
        log.error("❌ Error reading data from temporary stage.")
        raise ValueError("Error reading data from temporary stage.")

    # (6) Execute COPY INTO with retry logic for reliability
    max_retries = 3
    attempt = 0
    copied = False
    copied_into_result, qid = None, None
    fq_target_table = f"{database_name}.{schema_name}.{target_table}"

    while attempt < max_retries and not copied:
        try:
            copied_into_result = df.copy_into_table(
                fq_target_table,
                target_columns=target_columns,
                force=True,
                on_error=on_error,
                format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},
            )
            copied = True
            qid = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
        except Exception as e:
            attempt += 1
            log.warning(f"⚠️ COPY INTO failed attempt {attempt} of {max_retries}: {e}")
            time.sleep(2**attempt)

        if attempt == max_retries:
            log.error("❌ Max retries reached. Aborting copy operation.")
            raise ValueError("Max retries reached. Aborting copy operation.") from e

    # (7) Final status and return copy result
    if qid is None and log:
        log.warning("⚠️ No COPY query ID found — downstream reject handling may fail.")
    else:
        log.info(
            f"✅ COPY INTO {fq_target_table} succeeded after {attempt + 1} attempt(s). Query ID: {qid}"
        )

    return copied_into_result, qid
