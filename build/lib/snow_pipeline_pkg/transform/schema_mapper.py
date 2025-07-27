from snowflake.snowpark import DataFrame
from typing import Dict
import logging
from typing import List


def apply_column_mapping(
    df: DataFrame, mapping: Dict[str, str], log: logging.Logger
) -> DataFrame:
    """
    Renames DataFrame columns based on provided mapping.

    Args:
        df (DataFrame): Snowpark DataFrame with raw column names.
        mapping (Dict[str, str]): Dictionary mapping raw → target column names.
        log (Logger): Logger for structured output.

    Returns:
        DataFrame: Renamed DataFrame with updated columns.
    """

    # (1) Capture current DataFrame columns into a set for lookup
    source_cols = set(df.columns)

    # (2) Iterate through mapping dictionary to rename columns
    for raw, mapped in mapping.items():
        src = raw.strip('"').upper()
        tgt = mapped.strip('"').upper()

        # (3) If source column exists, rename it; else log a warning
        if src in source_cols:
            df = df.with_column_renamed(src, tgt)
            log.debug(f"🔄 Renamed '{src}' → '{tgt}'")
        else:
            log.warning(f"⚠️ Column '{src}' not found — skipped")

    # (4) Return DataFrame with updated column names
    return df


def drop_unmapped_columns(
    df: DataFrame, mapped_columns: List[str], log: logging.Logger
) -> DataFrame:
    """
    Drops columns from the DataFrame that aren’t listed in mapped_columns.

    Args:
        df (DataFrame): Snowpark DataFrame with renamed columns.
        mapped_columns (List[str]): List of column names to retain.
        log (Logger): Logger instance for structured diagnostics.

    Returns:
        DataFrame: Trimmed DataFrame with only mapped columns preserved.
    """

    # (1) Log current DataFrame state before column drop
    log.debug(f"🧪 DataFrame columns before drop: {df.columns}")
    log.debug(f"✅ Mapped columns: {mapped_columns}")

    # (2) Identify columns to retain vs drop
    current_cols = df.columns
    cols_to_keep = set(mapped_columns)
    cols_to_drop = [col for col in current_cols if col not in cols_to_keep]

    # (3) Validate mapped column list before executing drop
    if not cols_to_keep:
        log.error("❌ Mapped column list is empty — cannot drop all columns.")
        raise ValueError("Mapped column list is empty.")

    if len(cols_to_keep.intersection(current_cols)) == 0:
        log.error("❌ No mapped columns found in DataFrame — would drop all columns.")
        raise ValueError("No mapped columns present in DataFrame.")

    # (4) Drop unmapped columns individually with log output
    for col_name in cols_to_drop:
        log.debug(f"🗑️ Dropping unmapped column: {col_name}")
        df = df.drop(col_name)

    # (5) Return updated DataFrame
    return df
