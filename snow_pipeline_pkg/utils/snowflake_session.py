from snowflake.snowpark import Session

from snowflake.snowpark.functions import col, to_timestamp

from snowflake.snowpark.functions import current_timestamp, lit

from datetime import datetime



#  This is Cory's stuff ------------------------------------------------

from cryptography.hazmat.primitives import serialization

from cryptography.hazmat.backends import default_backend

from contextlib import contextmanager

from snow_pipeline_pkg.utils.connection_loader import load_connection_config





# Ensure logs directory exists

# log_dir = "logs"

# os.makedirs(log_dir, exist_ok=True)



# Configure logging to output to logs/session_closure.log

# log_file = os.path.join(log_dir, "session_closure.log")

# logging.basicConfig(

#     filename=log_file,

#     level=logging.INFO,

#     format="%(asctime)s - %(levelname)s - %(message)s",

# )



# from snow_pipeline_pkg.utils.log_utils import setup_logger



# log = setup_logger(log_to_file=True, log_filename="logs/pipeline.log")

log = logging.getLogger(__name__)



# log = logging.getLogger("session_logger")





# def managed_snowflake_session(config_path="./config/connection_details.json", log=None):

@contextmanager

def managed_snowflake_session(config_path=None, log=None):

    """

    Context manager for Snowflake session lifecycle.



    Loads connection config, creates a Snowpark session,

    yields it to the caller, and ensures proper cleanup on exit.



    Args:

        config_path (str): Path to the JSON file containing connection details.

    Yields:

        session (Session): Active Snowpark session object.

    """

    log = log or logging.getLogger(__name__)

    if config_path is None:

        config_path = os.getenv("SNOWFLAKE_CONFIG", "./config/connection_details.json")

    config = load_connection_config(config_path)

    session = snowconnection(config, log)

    try:

        yield session

    finally:

        close_session(session, log)





# def load_connection_config(path="./config/connection_details.json"):

#     """

#     Loads Snowflake connection config and injects private key bytes for key-pair authentication.



#     Reads the config JSON and private key file, then returns a config dict

#     ready for use with Snowpark's Session builder.



#     Args:

#         path (str): Path to the connection details JSON file.

#     Returns:

#         dict: Modified connection config with private key bytes added.

#     """

#     with open(path, "r") as f:

#         config = json.load(f)



#     private_key_path = config.get(

#         "private_key_path", "./config/snowflake_private_key.p8"

#     )



#     if not os.path.exists(private_key_path):

#         raise FileNotFoundError(f"❌ RSA key not found at: {private_key_path}")



#     with open(private_key_path, "rb") as key_file:

#         private_key = serialization.load_pem_private_key(

#             key_file.read(),

#             password=None,

#             backend=default_backend(),

#         )



#     pk_bytes = private_key.private_bytes(

#         encoding=serialization.Encoding.DER,

#         format=serialization.PrivateFormat.PKCS8,

#         encryption_algorithm=serialization.NoEncryption(),

#     )



#     config["private_key"] = pk_bytes

#     config.pop("private_key_path", None)



#     return config





def snowconnection(connection_config, log):

    """

    Creates and returns a Snowpark session and logs session metadata to 'SESSION_AUDIT' table.



    Args:

        connection_config (dict): Snowflake connection parameters.

    Returns:

        Session: Active Snowpark session.

    """

    log = log or logging.getLogger(__name__)

    log.info("🔗 Creating Snowpark session...")

    session = Session.builder.configs(connection_config).create()

    placeholder_time = datetime(2999, 1, 1)

    # STEP 1: Just build the initial 4-column DataFrame

    session_details = session.create_dataframe(

        [

            [

                session._session_id,

                session.sql("SELECT CURRENT_USER()").collect()[0][0],

                str(session.get_current_warehouse()).replace('"', ""),

                str(session.get_current_role()).replace('"', ""),

            ]

        ],

        schema=["SESSION_ID", "USER_NAME", "WAREHOUSE", "ROLE"],

    )



    # STEP 2: Add 2 more columns

    session_details = (

        session_details.with_column("CREATED_AT", current_timestamp())

        .with_column("CLOSED_AT", lit(placeholder_time))

        .select(

            "SESSION_ID", "USER_NAME", "WAREHOUSE", "ROLE", "CREATED_AT", "CLOSED_AT"

        )

    )



    # STEP 3: NOW write to the table

    session_details.write.mode("append").save_as_table("SESSION_AUDIT")

    log.info("🧊 Session audit logged with:")

    try:

        log.info(session_details.to_pandas().to_string())

    except Exception as _:

        log.warning(

            f"⚠️ Could not convert session details to pandas DataFrame so session audit was skipped: {e}"

        )

    return session





def close_session(session: Session, log):

    """

    Updates 'CLOSED_AT' timestamp in the SESSION_AUDIT table and closes the Snowpark session.



    Args:

        session (Session): Active Snowpark session.

    """

    log = log or logging.getLogger(__name__)



    try:

        session_id = session._session_id



        update_sql = f"""

        UPDATE SESSION_AUDIT

        SET CLOSED_AT = CURRENT_TIMESTAMP()

        WHERE SESSION_ID = {session_id}

    """

        session.sql(update_sql).collect()



        log.info(f"🛑 Session {session_id} closed and audit updated.")



    except Exception as _:

        try:

            # logging.error(f"⚠️ Error during session closure logic: {e}")

            log.error(f"⚠️ Error during session closure logic: {e}")

        except Exception:

            print(f"⚠️ [Fallback] Could not log error: {e}")

    finally:

        try:

            session.close()

        except Exception as close_err:

            log.error(f"❌ Failed to fully close session: {close_err}")





# def copy_to_table(session, config_file, schema="NA"):

#     database_name = config_file.get("Database_name")

#     Schema_name = config_file.get("Schema_name")

#     Target_table = config_file.get("Target_table")

#     target_columns = config_file.get("target_columns")

#     on_error = config_file.get("on_error")

#     Source_location = config_file.get("Source_location")

#     format_options = config_file.get("format_type_options", {})



#     if config_file.get("Source_file_type") == "csv":

#         # schema = schema # commented out this line from the forked code as not needed

#         df = session.read.schema(schema).csv("'" + Source_location + "'")



#     with session.query_history() as query_history:

#         copied_into_result = df.copy_into_table(

#             database_name + "." + Schema_name + "." + Target_table,

#             target_columns=target_columns,

#             force=True,

#             on_error=on_error,

#             format_type_options=format_options

#         )

#     query = query_history.queries



#     # Mention command to collect query id of copy command executed.

#     for id in query:

#         if "COPY" in id.sql_text:

#             qid = id.query_id

#     return copied_into_result, qid



# The stuff below was unchanged from the udemy forked code -------------

# def collect_rejects(session, qid, config_file):

#     database_name = config_file.get("Database_name")

#     Schema_name = config_file.get("Schema_name")

#     Target_table = config_file.get("Target_table")

#     Reject_table = config_file.get("Reject_table")

#     rejects = session.sql(

#         "select *  from table(validate("

#         + database_name

#         + "."

#         + Schema_name

#         + "."

#         + Target_table

#         + " , job_id =>"

#         + "'"

#         + qid

#         + "'))"

#     )

#     rejects.write.mode("append").save_as_table(Reject_table)

#     return rejects





# # def map_columns(df, map_columns):

# #     # Remove double qoutes from the column names and drop unwanted columns

# #     cols = df.columns

# #     map_keys = [key.upper() for key in map_columns.keys()]

# #     for c in cols:

# #         df = df.with_column_renamed(c, c.replace('"', ""))

# #     cols = df.columns

# #     for c in cols:

# #         if c.upper() not in map_keys:

# #             # print("Dropped column," + " " + c.upper())

# #             df = df.drop(c.upper())



# #     # Rename the dataframe column names

# #     for k, v in map_columns.items():

# #         df = df.with_column_renamed(k.upper(), v.upper())

# #     return df





# def copy_to_table_semi_struct_data(session, config_file, schema="NA"):

#     database_name = config_file.get("Database_name")

#     Schema_name = config_file.get("Schema_name")

#     Target_table = config_file.get("Target_table")

#     target_columns = config_file.get("target_columns")

#     on_error = config_file.get("on_error")

#     Source_location = config_file.get("source_location")

#     transformations = config_file.get("transformations")

#     maped_columns = config_file.get("map_columns")



#     if config_file.get("Source_file_type") == "csv":

#         return "Expecting semi structured data but got csv"

#     elif config_file.get("Source_file_type") == "avro":

#         df = session.read.avro(Source_location)



#     # Map columns in df to target table

#     df = map_columns(df, maped_columns)



#     # Create temporary stage

#     _ = session.sql("create or replace temp stage demo_db.public.mystage").collect()

#     remote_file_path = "@demo_db.public.mystage/" + Target_table + "/"

#     # Write df to temporary internal stage location

#     df.write.copy_into_location(

#         remote_file_path,

#         file_format_type="csv",

#         format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},

#         header=False,

#         overwrite=True,

#     )



#     # Read the file from temp stage location

#     df = session.read.schema(schema).csv("'" + remote_file_path + "'")

#     with session.query_history() as query_history:

#         copied_into_result = df.copy_into_table(

#             database_name + "." + Schema_name + "." + Target_table,

#             target_columns=target_columns,

#             force=True,

#             on_error=on_error,

#             format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY": '"'},

#         )

#     query = query_history.queries

#     # Mention command to collect query id of copy command executed.

#     for id in query:

#         if "COPY" in id.sql_text:

#             qid = id.query_id

#     return copied_into_result, qid



# def validate_schema_matches_table(df, expected_columns):

#     df_cols = set([c.upper() for c in df.columns])

#     target_cols = set([c.upper() for c in expected_columns])

#     missing = target_cols - df_cols

#     extras = df_cols - target_cols

#     return missing, extras

