from snowflake.snowpark import DataFrame
from typing import List, Tuple
import logging
from snow_pipeline_pkg.config.schemas import schema_registry


def validate_column_overlap(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    {function not used in the current codebase}
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
        log.info("✅ All source columns match config target columns.")

    return missing, extras


def validate_config_against_schema_ref(
    target_columns: List[str],
    schema_ref: str,
    log: logging.Logger,
) -> Tuple[List[str], List[str]]:
    """
    Compares a list of config-defined target columns against a registered schema reference.

    Args:
        target_columns (List[str]): List of target columns from the config file.
        schema_ref (str): Schema reference in the format 'schema_key@version'.
        log (Logger): Logger instance for structured messaging.

    Returns:
        Tuple[List[str], List[str]]:
            - missing: Columns found in the schema but missing from config.
            - extra: Columns found in config but not defined in schema.
    """

    # (1) Parse schema reference string into key and version
    schema_key, _, schema_version = schema_ref.partition("@")

    # (2) Look up schema definition and available versions from registry
    schema = schema_registry.get(schema_key, {}).get(schema_version)
    available_versions = list(schema_registry.get(schema_key.lower(), {}).keys())

    # (3) Log intent to validate against the schema reference
    log.info(
        f"🔍 Validating target columns against schema reference '{schema_ref}' "
        f"with available versions: {available_versions}"
    )

    # (4) Raise error if requested version doesn’t exist
    if schema_version and schema_version not in available_versions:
        log.warning(
            f"⚠️ Schema version '{schema_version}' not found for '{schema_key}'. "
            f"Available versions: {available_versions}"
        )
        raise ValueError(
            f"Schema version '{schema_version}' not found for '{schema_key}'."
        )

    # (5) Handle missing schema object gracefully
    if not schema:
        log.warning(f"⚠️ No schema found for reference: {schema_ref}")
        return [], []

    # (6) Normalize columns: schema fields vs config target columns
    expected_columns = [field.name.upper() for field in schema.fields]
    target_cols = [col.upper() for col in target_columns]

    # (7) Compare sets to identify schema mismatches
    missing = sorted(set(expected_columns) - set(target_cols))
    extra = sorted(set(target_cols) - set(expected_columns))

    # (8) Log mismatch results or validation success
    if missing:
        log.warning(f"⚠️ Columns missing from config: {missing}")
    if extra:
        log.warning(f"⚠️ Extra columns in config not found in schema: {extra}")
    if not missing and not extra:
        log.info(
            f"✅ Config target columns match schema reference '{schema_ref}' exactly."
        )

    # (9) Return results for downstream validation
    return missing, extra
