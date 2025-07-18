from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType

# from transform.schema_mapper import map_columns
import os
import logging
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

        # 👇 Insert this block here
        empty_schema = StructType(
            [
                StructField("ROW_STATUS", StringType()),
                StructField("ERROR_MESSAGE", StringType()),
                StructField("RAW_ROW", StringType()),
            ]
        )
        return session.create_dataframe([], schema=empty_schema)

    # 🏗️ Continue as normal
    database_name = config_file.get("Database_name")
    schema_name = config_file.get("Schema_name")
    target_table = config_file.get("Target_table")
    reject_table = config_file.get("Reject_table")

    rejects = session.sql(
        "select * from table(validate("
        + database_name
        + "."
        + schema_name
        + "."
        + target_table
        + ", job_id => '"
        + qid
        + "'))"
    )

    rejects.write.mode("append").save_as_table(reject_table)
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

    # Map columns in df to target table
    df = apply_column_mapping(df, mapped_columns, log)
    df = drop_unmapped_columns(df, mapped_columns, log)

    # Create temporary stage
    _ = session.sql("create or replace temp stage demo_db.public.mystage").collect()
    remote_file_path = "@demo_db.public.mystage/" + target_table + "/"
    # Write df to temporary internal stage location
    df.write.copy_into_location(
        remote_file_path,
        file_format_type="csv",
        format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},
        header=False,
        overwrite=True,
    )

    # Read the file from temp stage location
    # df = session.read.schema(schema).csv("'" + remote_file_path + "'")
    df = session.read.csv("'" + remote_file_path + "'")
    with session.query_history() as query_history:
        copied_into_result = df.copy_into_table(
            database_name + "." + schema_name + "." + target_table,
            target_columns=target_columns,
            force=True,
            on_error=on_error,
            format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},
        )
    query = query_history.queries
    # This block of code is responsible for extracting the query ID of the `COPY INTO` command that
    # was executed during the data copy process. Here's a breakdown of what it does:
    # Mention command to collect query id of copy command executed.
    qid = None
    for id in query:
        if "COPY INTO " in id.sql_text.upper():
            qid = id.query_id
    if qid is None and log:
        log.warning("⚠️ No COPY query ID found — downstream reject handling may fail.")
    return copied_into_result, qid
