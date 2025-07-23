from snowflake.snowpark import DataFrame
from typing import List, Tuple
import logging
from config.schemas import schema_registry


# def validate_column_overlap(df: DataFrame, expected_columns: List[str]) -> bool:
#     """
#     Checks if DataFrame columns overlap with expected schema columns (case-insensitive).

#     Args:
#         df (DataFrame): Snowpark DataFrame.
#         expected_columns (List[str]): List of expected column names.

#     Returns:
#         bool: True if there is any column overlap; False otherwise.
#     """
#     df_cols = set(c.upper() for c in df.columns)
#     expected_set = set(c.upper() for c in expected_columns)

#     overlap = df_cols & expected_set
#     return bool(overlap)


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
        log.info("✅ All df columns match config target columns.")

    return missing, extras


def validate_config_against_schema_ref(
    target_columns: List[str],
    schema_ref: str,
    log: logging.Logger,
) -> Tuple[List[str], List[str]]:
    schema_key, _, schema_version = schema_ref.partition("@")
    schema = schema_registry.get(schema_key, {}).get(schema_version)
    available_versions = list(schema_registry.get(schema_key.lower(), {}).keys())
    # log.warning(f"⚠️ No schema found for '{schema_key}@{schema_version}'. Available versions: {available_versions}")
    log.info(
        f"🔍 Validating target columns against schema reference '{schema_ref}' with available versions: {available_versions}"
    )
    if schema_version and schema_version not in available_versions:
        log.warning(
            f"⚠️ Schema version '{schema_version}' not found for '{schema_key}'. Available versions: {available_versions}"
        )
        raise ValueError(
            f"Schema version '{schema_version}' not found for '{schema_key}'."
        )
    if not schema:
        log.warning(f"⚠️ No schema found for reference: {schema_ref}")
        return [], []

    expected_columns = [field.name.upper() for field in schema.fields]
    target_cols = [col.upper() for col in target_columns]

    missing = sorted(set(expected_columns) - set(target_cols))
    extra = sorted(set(target_cols) - set(expected_columns))

    if missing:
        log.warning(f"⚠️ Columns missing from config: {missing}")
    if extra:
        log.warning(f"⚠️ Extra columns in config not found in schema: {extra}")
    if not missing and not extra:
        log.info(
            f"✅ Config target columns match schema reference '{schema_ref}' exactly."
        )

    return missing, extra
