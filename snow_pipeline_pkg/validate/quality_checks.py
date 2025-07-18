from snowflake.snowpark import DataFrame

from typing import List, Tuple





def validate_column_overlap(df: DataFrame, expected_columns: List[str]) -> bool:

    """

    Checks if DataFrame columns overlap with expected schema columns (case-insensitive).



    Args:

        df (DataFrame): Snowpark DataFrame.

        expected_columns (List[str]): List of expected column names.



    Returns:

        bool: True if there is any column overlap; False otherwise.

    """

    df_cols = set(c.upper() for c in df.columns)

    expected_set = set(c.upper() for c in expected_columns)



    overlap = df_cols & expected_set

    return bool(overlap)





def validate_schema_matches_table(

    df: DataFrame, expected_columns: List[str], log: logging.Logger

) -> Tuple[List[str], List[str]]:

    """

    Validates whether the DataFrame columns match expected target table columns.



    Args:

        df (DataFrame): Snowpark DataFrame after transformation.

        expected_columns (List[str]): List of expected column names from the config.

        log (Logger): Logger for structured messages.



    Returns:

        Tuple[List[str], List[str]]:

            - missing: columns in target schema not found in source DataFrame

            - extras: columns in DataFrame not expected in target schema

    """

    df_cols = set([c.upper() for c in df.columns])

    target_cols = set([c.upper() for c in expected_columns])



    missing = sorted(target_cols - df_cols)

    extras = sorted(df_cols - target_cols)



    if missing:

        log.warning(f"⚠️ Missing expected columns: {missing}")

    if extras:

        log.warning(f"⚠️ Unused columns from source: {extras}")

    if not missing and not extras:

        log.info("✅ All schema columns matched.")



    return missing, extras

