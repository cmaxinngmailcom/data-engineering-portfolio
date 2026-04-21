# ============================================================
# TEST EXTRACT MODULE (test_extract.py)
# ============================================================
# PURPOSE:
# This module contains unit tests for the extract module.
#
# FIRST GOAL (learning phase):
# - Validate that CSV extraction works correctly and safely.
#
# WHAT SHOULD LIVE HERE:
# - Tests for extract_csv()
# - Validation of DataFrame structure and content
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "Does the extraction layer correctly read and return data?"
#
# BEST PRACTICES:
# - Use temporary files (tmp_path) instead of real data files
# - Keep tests isolated and independent
# - Avoid reliance on external systems or large datasets
# - Ensure tests are fast and repeatable
#
# WHAT SHOULD NOT BE HERE:
# - ❌ Database connections
# - ❌ Data transformation logic
# - ❌ Full ETL pipeline execution
#
# END RESULT:
# Ensures the extract layer is reliable and correctly loads data
# into pandas DataFrames, preventing issues before transformation
# and loading stages.
#
# ============================================================

# ============================================================
# TEST EXTRACT MODULE
# ============================================================
# Handles:
# - Testing CSV file reading
# - Validating DataFrame output
# ============================================================

# ============================================================
# TEST HELPER FUNCTIONS
# ============================================================
# test_extract_csv():
# - Ensures CSV file is correctly read into a DataFrame
# - Validates row count and column structure
#
# DESIGN PRINCIPLE:
# 👉 "Tests validate behavior, not implementation details."
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: How did you test your extract layer?
#
# A:
# I implemented unit tests for the extraction layer using pytest
# and temporary file fixtures (tmp_path). This allowed me to simulate
# CSV input files without relying on external datasets, ensuring that
# the extraction logic is reliable, isolated, and easy to maintain.
#
# This approach keeps tests fast, repeatable, and independent of
# production data sources.
# ============================================================
# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why test the extract layer separately?
#
# A:
# I tested the extraction layer independently to ensure CSV input
# files are correctly loaded into pandas DataFrames before any
# transformation or loading occurs. This isolates failures early
# in the ETL pipeline and improves debugging efficiency.
# ============================================================


from athletes_etl.extract import extract_csv
import pandas as pd

def test_extract_csv(tmp_path):
    # Step 1: create fake CSV file
    file = tmp_path / "test.csv"
    file.write_text("id,name\n1,John\n2,Jane")

    # Step 2: call function
    df = extract_csv(file)

    # Step 3: validate result
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]