# ============================================================
# TRANSFORM MODULE (transform.py)
# ============================================================
# PURPOSE:
# This module is responsible for data transformation and validation
# logic within the ETL pipeline.
#
# FIRST GOAL (learning phase):
# - Move transformation-related logic out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - Data cleansing (string trimming, null handling)
# - Type conversions (dates, numerics)
# - Helper functions (e.g., split_rejects)
# - Deduplication logic (hash-based or business key)
# - Later: reusable validation/reject helper functions
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How do I clean, validate, and prepare data for loading?"
#
# BEST PRACTICES:
# - Keep transformation logic separate from extraction and DB logic
# - Write reusable, testable functions (pure functions when possible)
# - Avoid side effects (do not write to DB here)
# - Return clean DataFrames (input → output)
#
# WHAT SHOULD NOT BE HERE:
# - ❌ CSV file reading (belongs in extract.py)
# - ❌ Database writes (belongs in db.py)
# - ❌ Full ETL orchestration (belongs in main.py)
#
# END RESULT:
# main.py should NOT contain heavy transformation logic.
# Instead, it should call functions from this module like:
#
#     from athletes_etl.transform import clean_athletes, dedupe_rows
#
# DESIGN PRINCIPLE:
# 👉 "transform.py prepares the data, it does not move or store it."
#
# ============================================================


# ============================================================
# TRANSFORM MODULE
# ============================================================
# Handles:
# - Data cleansing and type conversion
# - Validation helpers and reject splitting
# - Deduplication logic
# ============================================================


# ============================================================
# TRANSFORM HELPER FUNCTIONS
# ============================================================
# Functions in this module operate on DataFrames and return
# transformed results without performing any I/O operations.
#
# WHY MOVE THEM HERE:
# - Keeps main.py clean and readable
# - Centralizes transformation logic for reuse and testing
# - Separates business rules from database and extraction layers
#
# EXAMPLES:
# - clean_athletes(df)
# - split_rejects(df, mask)
# - dedupe_rows(df, hash_cols)
#
# ============================================================


# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why did you introduce a transform.py module?
#
# A:
# I introduced a transform module to separate data transformation
# and validation logic from orchestration and database access.
# This improves maintainability, readability, and reusability,
# while making the pipeline easier to test and extend.
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: What did you do after stabilizing db.py and main.py?
#
# A:
# After stabilizing the database layer and confirming the pipeline
# still ran cleanly, the next step was to introduce a transform
# module. I used it to gradually separate data cleansing,
# type conversions, and deduplication logic from the orchestration
# layer, while keeping the ETL flow stable during refactoring.
# ============================================================

# ============================================================
# TRANSFORM FUNCTIONS (Phase 1)
# ============================================================

import pandas as pd
import hashlib


def clean_athletes_data(df: pd.DataFrame, default_date) -> pd.DataFrame:
    """
    Cleans athlete data:
    - trims strings
    - parses dates
    - converts numerics
    """

    df = df.copy()

    # -------------------------
    # STRING CLEANING
    # -------------------------
    string_cols = ["name", "born_city", "born_region", "born_country", "NOC"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").str.strip()

    # -------------------------
    # DATE CLEANING
    # -------------------------
    date_cols = ["born_date", "died_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .fillna(default_date)
                .dt.date
            )

    # -------------------------
    # NUMERIC CLEANING
    # -------------------------
    numeric_cols = ["height_cm", "weight_kg"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ============================================================
# SPLIT REJECTS
# ============================================================

def split_rejects(df: pd.DataFrame, mask: pd.Series):
    """
    Splits dataframe into:
    - good rows (valid)
    - bad rows (rejected)
    """
    bad = df.loc[mask].copy()
    good = df.loc[~mask].copy()
    return good, bad


# ============================================================
# DEDUPLICATION
# ============================================================

def generate_row_hash(row, hash_cols) -> str:
    combined = "|".join(str(row.get(col, "")) for col in hash_cols)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def dedupe_rows(df: pd.DataFrame, hash_cols):
    """
    Removes duplicates using hash-based comparison.
    Returns:
    - deduped dataframe
    - number of duplicates removed
    """

    df = df.copy()

    df["row_hash"] = df.apply(lambda r: generate_row_hash(r, hash_cols), axis=1)

    dupes = int(df.duplicated("row_hash").sum())

    df = df.drop_duplicates(subset="row_hash").drop(columns=["row_hash"])

    return df, dupes

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why keep the DEDUPE_ENABLED condition in main.py instead of transform.py?
#
# A:
# I keep control flow and configuration-driven decisions in the
# orchestration layer (main.py), while the transformation module
# focuses only on pure data logic. This ensures better separation
# of concerns and makes transformation functions reusable and testable.
# ============================================================

# ============================================================
# REJECT RULE HELPERS
# ============================================================
# PURPOSE:
# These functions build boolean masks and helper outputs used
# to identify invalid rows during transformation.
#
# WHY THEY BELONG IN transform.py:
# - They are data-validation / transformation logic
# - They do not perform database writes
# - They keep main.py cleaner and easier to read
#
# DESIGN PRINCIPLE:
# transform.py determines which rows are invalid
# db.py persists those invalid rows
# main.py orchestrates the flow
# ============================================================

import pandas as pd


def build_string_length_rejects(df: pd.DataFrame, string_limits: dict):
    """
    Returns:
    - too_long_mask: boolean mask of rows that exceed SQL string limits
    - reasons: Series containing per-row reason details
    """
    too_long_mask = pd.Series(False, index=df.index)
    reasons = pd.Series("", index=df.index, dtype="string")

    for col, limit in string_limits.items():
        if col in df.columns:
            s = df[col].astype("string").fillna("")
            lens = s.str.len()
            col_too_long = lens > int(limit)

            too_long_mask = too_long_mask | col_too_long

            reasons.loc[col_too_long] = (
                reasons.loc[col_too_long].fillna("")
                + f"{col} length "
                + lens.loc[col_too_long].astype(str)
                + f" > {int(limit)}; "
            )

    return too_long_mask, reasons


def build_numeric_reject_mask(
    df: pd.DataFrame,
    height_min: int,
    height_max: int,
    height_allow_zero: bool,
    weight_min: int,
    weight_max: int,
    weight_allow_zero: bool,
) -> pd.Series:
    """
    Build numeric reject mask for height_cm and weight_kg.
    """
    numeric_reject_mask = pd.Series(False, index=df.index)

    if "height_cm" in df.columns:
        if height_allow_zero:
            numeric_reject_mask = numeric_reject_mask | (
                (df["height_cm"] != 0)
                & ((df["height_cm"] < height_min) | (df["height_cm"] > height_max))
            )
        else:
            numeric_reject_mask = numeric_reject_mask | (
                (df["height_cm"] < height_min) | (df["height_cm"] > height_max)
            )

    if "weight_kg" in df.columns:
        if weight_allow_zero:
            numeric_reject_mask = numeric_reject_mask | (
                (df["weight_kg"] != 0)
                & ((df["weight_kg"] < weight_min) | (df["weight_kg"] > weight_max))
            )
        else:
            numeric_reject_mask = numeric_reject_mask | (
                (df["weight_kg"] < weight_min) | (df["weight_kg"] > weight_max)
            )

    return numeric_reject_mask


def build_date_reject_mask(
    df: pd.DataFrame,
    today,
    min_born,
    default_date_str: str,
    reject_born_after_today: bool,
    reject_died_before_born: bool,
) -> pd.Series:
    """
    Build date sanity reject mask.
    """
    born_ts = (
        pd.to_datetime(df["born_date"], errors="coerce")
        if "born_date" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )

    died_ts = (
        pd.to_datetime(df["died_date"], errors="coerce")
        if "died_date" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )

    default_died = pd.Timestamp(default_date_str)
    date_reject_mask = pd.Series(False, index=df.index)

    if reject_born_after_today:
        date_reject_mask = date_reject_mask | (born_ts > today)

    date_reject_mask = date_reject_mask | (born_ts < min_born)

    if reject_died_before_born:
        date_reject_mask = date_reject_mask | (
            (died_ts.notna()) & (died_ts != default_died) & (died_ts < born_ts)
        )

    return date_reject_mask

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why move reject-mask logic into transform.py?
#
# A:
# I moved reject-mask building logic into the transformation layer
# because it is part of data validation and preparation, not database
# access. This keeps the pipeline modular: transform.py identifies
# invalid rows, db.py writes them, and main.py orchestrates the flow.
# ============================================================