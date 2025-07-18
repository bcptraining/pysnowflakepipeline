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
    source_cols = set(df.columns)

    for raw, mapped in mapping.items():
        src = raw.strip('"').upper()
        tgt = mapped.strip('"').upper()
        if src in source_cols:
            df = df.with_column_renamed(src, tgt)
            log.info(f"🔄 Renamed '{src}' → '{tgt}'")
        else:
            log.warning(f"⚠️ Column '{src}' not found — skipped")

    return df


def drop_unmapped_columns(
    df: DataFrame, mapped_columns: List[str], log: logging.Logger
) -> DataFrame:
    log.debug(f"🧪 DataFrame columns before drop: {df.columns}")
    log.debug(f"✅ Mapped columns: {mapped_columns}")

    current_cols = df.columns
    cols_to_keep = set(mapped_columns)
    cols_to_drop = [col for col in current_cols if col not in cols_to_keep]

    if not cols_to_keep:
        log.error("❌ Mapped column list is empty — cannot drop all columns.")
        raise ValueError("Mapped column list is empty.")

    if len(cols_to_keep.intersection(current_cols)) == 0:
        log.error("❌ No mapped columns found in DataFrame — would drop all columns.")
        raise ValueError("No mapped columns present in DataFrame.")

    for col_name in cols_to_drop:
        log.info(f"🗑️ Dropping unmapped column: {col_name}")
        df = df.drop(col_name)

    return df
