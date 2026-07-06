# ============================================================
# VALIDATOR MODULE (validator.py)
# ============================================================
# PURPOSE:
# This module is responsible for validating and cleaning data
# before it is loaded into SQL Server.
#
# FIRST GOAL (learning phase):
# - Move validation logic out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - Data cleaning
# - String length validation
# - Numeric range validation
# - Date sanity validation
# - Duplicate detection
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "Is this data valid before loading?"
#
# BEST PRACTICES:
# - Keep validation rules together
# - Return clean DataFrames
# - Return rejected rows separately
# - Never connect to SQL Server here
#
# WHAT SHOULD NOT BE HERE:
# - ❌ Database connections
# - ❌ CSV extraction
# - ❌ ETL orchestration
# - ❌ Logging configuration
#
# END RESULT:
# main.py should simply call validator functions instead of
# containing hundreds of validation lines.
#
# DESIGN PRINCIPLE:
# 👉 "validator.py validates data—not databases."
#
# ============================================================

import pandas as pd


# ============================================================
# CLEAN DATA
# ============================================================

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning before validation.
    """

    df = df.copy()

    # Trim whitespace from string columns
    string_cols = df.select_dtypes(include="object").columns

    for col in string_cols:
        df[col] = df[col].fillna("").str.strip()

    return df


# ============================================================
# STRING LENGTH VALIDATION
# ============================================================

def string_length_mask(df: pd.DataFrame, rules: dict):
    """
    Returns rows violating maximum string lengths.
    """

    reject_mask = pd.Series(False, index=df.index)

    for column, max_length in rules.items():

        if column not in df.columns:
            continue

        reject_mask |= (
            df[column]
            .astype(str)
            .str.len()
            > max_length
        )

    return reject_mask


# ============================================================
# NUMERIC RANGE VALIDATION
# ============================================================

def numeric_range_mask(df: pd.DataFrame,
                       column: str,
                       minimum: int,
                       maximum: int):

    return (
        (df[column] < minimum) |
        (df[column] > maximum)
    )


# ============================================================
# DATE SANITY VALIDATION
# ============================================================

def date_sanity_mask(df: pd.DataFrame):

    return (
        df["died_date"].notna()
        &
        df["born_date"].notna()
        &
        (df["died_date"] < df["born_date"])
    )


# ============================================================
# HASH DUPLICATES
# ============================================================

def remove_duplicates(df: pd.DataFrame,
                      subset: list):

    before = len(df)

    df = df.drop_duplicates(subset=subset)

    removed = before - len(df)

    return df, removed


# ============================================================
# SPLIT REJECTS
# ============================================================

def split_rejects(df: pd.DataFrame,
                  reject_mask):

    rejects = df[reject_mask].copy()

    valid = df[~reject_mask].copy()

    return valid, rejects


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
# Q:
# Why create a validator.py module?
#
# A:
# Separating validation rules from ETL orchestration makes the
# pipeline easier to maintain, test, and extend.
#
# Instead of embedding hundreds of validation lines inside
# main.py, validator.py centralizes all business rules.
#
# This follows the Single Responsibility Principle (SRP):
#
# extract.py
#     → Gets the data
#
# validator.py
#     → Validates the data
#
# db.py
#     → Talks to SQL Server
#
# logging_utils.py
#     → Handles logging
#
# main.py
#     → Orchestrates the pipeline
#
# This modular design is common in production ETL systems.
# ============================================================