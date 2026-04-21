# ============================================================
# EXTRACT MODULE (extract.py)
# ============================================================
# PURPOSE:
# This module is responsible for extracting data from source systems
# (starting with CSV files) and returning it in a usable format
# (pandas DataFrame).
#
# FIRST GOAL (learning phase):
# - Move extraction logic out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - extract_csv() function → reads CSV file into DataFrame
# - File existence validation (fail early if file is missing)
# - Optional parameters (encoding, delimiter, etc.)
# - Lightweight logging (e.g., print or logging)
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How do I safely read source data and return it?"
#
# BEST PRACTICES:
# - Validate that the source file exists before reading
# - Keep extraction logic simple (DO NOT mix with transformation)
# - Return clean DataFrame, do NOT modify business logic here
# - Handle file-related errors clearly (FileNotFoundError, encoding issues)
#
# WHAT SHOULD NOT BE HERE:
# - ❌ Data cleansing (string trimming, type casting, etc.)
# - ❌ Business rules or reject logic
# - ❌ Database operations
#
# END RESULT:
# main.py should NOT directly read files.
# Instead, it should call functions from this module like:
#
#     from athletes_etl.extract import extract_csv
#     df = extract_csv(csv_path)
#
# DESIGN PRINCIPLE:
# 👉 "Extraction is only responsible for reading data, not transforming it."
#
# FUTURE EXTENSIONS:
# - Add support for other sources (Excel, JSON, API, S3, etc.)
# - Add chunked reading for large files
# - Replace print with structured logging
#
# ============================================================


# ============================================================
# EXTRACT MODULE
# ============================================================
# Handles:
# - Reading source data from CSV
# - Validating that the file exists
# - Returning a pandas DataFrame
# ============================================================

from pathlib import Path
import pandas as pd

# I added encoding="utf-8" to make your function:
# explicit,flexible, production-ready,interview-friendly
# Pandas assumes UTF-8 by default
# If your file is NOT UTF-8 → you may get:
# UnicodeDecodeError: 'utf-8' codec can't decode byte...



def extract_csv(csv_path: str, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Read a CSV file and return a DataFrame.

    Args:
        csv_path: Full path to the CSV file
        encoding: File encoding (default: utf-8)

    Returns:
        pandas DataFrame
    """
    path = Path(csv_path)

    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    print(f"Reading CSV from: {path}")
    return pd.read_csv(path, encoding=encoding)

# ============================================================
# WHAT YOU LEARNED (extract.py)
# ============================================================
# This module should answer:
# 👉 "How do I safely read source data and return it to the pipeline?"
#
# That is its ONLY responsibility.
#
# - NOT cleaning data
# - NOT applying business rules
# - NOT loading into a database
#
# 👉 Just extraction.
#
# ------------------------------------------------------------
# GOOD RULE:
# Keep extract.py focused on:
# - Reading files (CSV, later Excel/API/etc.)
# - Checking file existence before reading
# - Handling file options (encoding, delimiter, etc.)
#
# DO NOT move transformation or cleansing logic here yet.
# That belongs in a separate step/module.
#
# ------------------------------------------------------------
# INTERVIEW NOTE (important concept)
#
# Q: Why include an encoding parameter?
#
# A:
# I included an encoding parameter to make the extraction function
# flexible and robust against different file encodings. In real-world
# pipelines, CSV files may come from different systems using different
# encodings, so making it configurable prevents decoding errors and
# data corruption.
#
# ============================================================