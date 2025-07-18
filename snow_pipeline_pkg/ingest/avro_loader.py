from snowflake.snowpark import Session

from snowflake.snowpark.functions import col

from typing import Dict





def normalize_column(column_name: str):

    """

    Normalize Avro column names by stripping quotes and converting to uppercase.



    Args:

        column_name (str): Raw column name from Avro source.

    Returns:

        Column: Renamed Snowpark Column object.

    """

    cleaned = column_name.strip('"')

    return col(f'"{cleaned}"').alias(cleaned.upper())





def load_avro_file(session: Session, config: Dict, log: logging.Logger):

    """

    Load Avro file from specified Snowflake stage location and normalize columns.



    Args:

        session (Session): Active Snowflake Snowpark session.

        config (Dict): Configuration dictionary with 'source_location' and 'Source_file_type'.

        log (Logger): Logger instance for structured logging.



    Returns:

        DataFrame: Snowpark DataFrame with normalized columns.

    """

    source_location = config.get("source_location")

    file_type = config.get("source_file_type", "").lower()



    if file_type != "avro":

        raise ValueError(f"❌ Unsupported file type: '{file_type}'. Expected 'avro'.")



    if not source_location:

        log.error("❌ source_location is missing from config.")

        raise ValueError("Missing 'source_location' in configuration.")



    log.info(f"📥 Reading Avro data from: {source_location}")

    try:

        df = session.read.avro(source_location)

    except Exception as avro_err:

        log.error(f"❌ Failed to read Avro file: {avro_err}")

        raise



    log.info(f"🔍 Avro file loaded with columns: {df.columns}")



    # Normalize column names (strip quotes, uppercase)

    df = df.select([normalize_column(c) for c in df.columns])

    log.info(f"🧼 Normalized columns: {df.columns}")



    return df

