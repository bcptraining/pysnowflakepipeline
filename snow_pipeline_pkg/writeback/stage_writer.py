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

# ---------- Utility Functions ----------
# def map_columns(df, map_columns):
#     # Remove double qoutes from the column names and drop unwanted columns
#     cols = df.columns
#     map_keys = [key.upper() for key in map_columns.keys()]
#     for c in cols:
#         df = df.with_column_renamed(c, c.replace('"', ""))
#     cols = df.columns
#     for c in cols:
#         if c.upper() not in map_keys:
#             # print("Dropped column," + " " + c.upper())
#             df = df.drop(c.upper())

#     # Rename the dataframe column names
#     for k, v in map_columns.items():
#         df = df.with_column_renamed(k.upper(), v.upper())
#     return df


# ---------- Collection rejects ----------
# def collect_rejects(session, qid, config_file, log):
#     if not qid:
#         log.warning("⚠️ No Query ID provided. Skipping reject collection.")
#         return session.create_dataframe([])  # Return empty frame gracefully
#     database_name = config_file.get("Database_name")
#     schema_name = config_file.get("Schema_name")
#     target_table = config_file.get("Target_table")
#     reject_table = config_file.get("Reject_table")
#     rejects = session.sql(
#         "select *  from table(validate("
#         + database_name
#         + "."
#         + schema_name
#         + "."
#         + target_table
#         + " , job_id =>"
#         + "'"
#         + qid
#         + "'))"
#     )
#     rejects.write.mode("append").save_as_table(reject_table)
#     return rejects
from snowflake.snowpark.types import StructType


def collect_rejects(session, qid, config_file, log):
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

    database_name = config_file.get("database_name")  # lowercase keys
    schema_name = config_file.get("schema_name")
    target_table = config_file.get("target_table")
    reject_table = config_file.get("reject_table")

    sql = f"""SELECT * FROM TABLE(
        VALIDATE(
            {database_name}.{schema_name}.{target_table},
            JOB_ID => '{qid}'
        )
    )"""

    log.debug(f"🧪 Running reject collection query: {sql}")
    rejects = session.sql(sql)

    # Save rejects to configured table
    rejects.write.mode("append").save_as_table(reject_table)
    log.info(f"✅ Rejects written to table: {reject_table}")

    return rejects


# ---------- Copy to table for semi-structured data ----------


def copy_to_table_semi_struct_data(session, config_file, df, schema="NA", log=None):
    database_name = config_file.get("database_name")
    schema_name = config_file.get("schema_name")
    target_table = config_file.get("target_table")
    target_columns = config_file.get("target_columns")
    on_error = config_file.get("on_error")
    source_location = config_file.get("source_location")
    transformations = config_file.get("transformations")
    mapped_columns = config_file.get("map_columns")
    source_type = config_file.get("source_file_type")

    # Read source file (if AVRO)
    # df = None  # Initialize early to appease static analysis (pylint)
    # if source_type == "csv":
    #     raise ValueError("❌ Expected semi-structured data (Avro), but got CSV.")
    # elif source_type == "parquet":
    #     raise ValueError("❌ Expected semi-structured data (Avro), but got parquet.")
    # elif source_type == "avro":
    #     if not Source_location:
    #         raise ValueError("❌ 'source_location' is missing in config.")
    #     df = session.read.avro(Source_location)
    #     log.info(f"📥 Loaded Avro file from: {Source_location}")
    # else:
    #     raise ValueError(f"❌ Unsupported source type: {source_type}")

    # Map columns in df to target table -- The mapping was impleted in te pipeline_runer so commenting out for now
    # df = apply_column_mapping(df, mapped_columns, log)
    # df = drop_unmapped_columns(df, mapped_columns, log)

    # Create temporary stage
    _ = session.sql(
        "create or replace temp stage demo_db.public.mystage"
    ).collect()  # make this dynamic based on copy config
    remote_file_path = "@demo_db.public.mystage/" + target_table + "/"
    # Write df to temporary internal stage location
    rows_to_be_written_to_temporary_stage = df.count()
    # log.debug(
    #     f"📤 Writing {rows_written_to_temporary_stage} rows from df to temporary stage: {remote_file_path}  with columns {df.columns}"
    # )
    copy_into_temp_stage_result = df.write.copy_into_location(
        remote_file_path,
        file_format_type="csv",
        format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},
        header=False,
        overwrite=True,
    )
    # Inspect the result
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
            f"✅ Successfully wrote {rows_successfully_written_to_temporary_stage} rows to temporary stage: {remote_file_path} wit columns {df.columns}"
        )
    # Read the file from temp stage location
    # df = session.read.schema(schema).csv("'" + remote_file_path + "'")

    df = session.read.csv("'" + remote_file_path + "'")
    rows_read_from_temporary_stage = df.count()
    log.debug(
        f"📤 Read {rows_read_from_temporary_stage} rows from temporary stage: {remote_file_path}"
        + f" with columns {df.columns}"
    )
    if rows_read_from_temporary_stage != rows_successfully_written_to_temporary_stage:
        log.error(
            "❌ Error reading data from temporary stage. Aborting copy operation."
        )
        raise ValueError("Error reading data from temporary stage.")

    # Perform the actual COPY INTO operation into target table (use retry logic w/exponential backoff
    max_retries = 3
    attempt = 0
    copied = False
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
            log.warning(
                f"⚠️ COPY INTO failed attempt {attempt} of {max_retries}.failed: {e}"
            )
            time.sleep(2**attempt)  # exponential backoff
        if attempt == max_retries:
            log.error("❌ Max retries reached. Aborting copy operation.")
            raise ValueError("Max retries reached. Aborting copy operation.") from e

    # This block of code is responsible for extracting the query ID of the `COPY INTO` command that
    # was executed during the data copy process. Here's a breakdown of what it does:
    # Mention command to collect query id of copy command executed.

    # Iterate through the query history to find the COPY INTO command  that was executed
    # and extract its query ID.
    # qid = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
    if qid is None and log:
        log.warning("⚠️ No COPY query ID found — downstream reject handling may fail.")
    else:
        log.info(
            f"✅ COPY INTO {fq_target_table} succeeded after {attempt + 1} attempt(s). Query ID: {qid}"
        )
        return copied_into_result, qid
