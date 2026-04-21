# ============================================================
# LOGGING MODULE (logging_utils.py)
# ============================================================
# PURPOSE:
# This module is responsible for centralized logging setup
# and logging helper functions for the ETL pipeline.
#
# FIRST GOAL (learning phase):
# - Move logging configuration out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - setup_logger() → configure logger for console + file output
# - Later: helper functions like log_etl_summary()
# - Later: helper functions like log_reject_summary()
# - Later: error logging wrappers
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How do I log ETL activity consistently and professionally?"
#
# BEST PRACTICES:
# - Keep logging configuration in one place
# - Use structured logging instead of scattered print()
# - Write logs both to console and file
# - Keep log format consistent across the pipeline
#
# WHAT SHOULD NOT BE HERE:
# - ❌ Database writes
# - ❌ Data transformations
# - ❌ Full ETL orchestration logic
#
# END RESULT:
# main.py should NOT configure raw logging directly.
# Instead, it should call:
#
#     from athletes_etl.logging_utils import setup_logger
#     logger = setup_logger()
#
# DESIGN PRINCIPLE:
# 👉 "logging_utils.py handles observability, not business logic."
#
# ============================================================

# ============================================================
# LOGGING MODULE
# ============================================================
# Handles:
# - Logger creation
# - Console logging
# - File logging
# - Standard log formatting
# ============================================================

# ============================================================
# LOGGING HELPER FUNCTION
# ============================================================
# setup_logger() belongs in logging_utils.py because it creates
# and configures the centralized logger used across the ETL app.
#
# WHY MOVE IT:
# - Keeps main.py cleaner
# - Centralizes reusable logging logic
# - Makes logging consistent across modules
#
# main.py should call:
#     logger = setup_logger()
#
# instead of defining logging config inline.
# ============================================================

import logging
import os


def setup_logger():
    """
    Creates and configures the ETL logger.
    Logs are written to:
    - Console
    - logs/etl.log file
    """

    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log"),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger("athletes_etl")

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why did you introduce centralized logging into your ETL pipeline?
#
# A:
# After stabilizing the ETL architecture and separating concerns
# across config.py, extract.py, db.py, and transform.py, I introduced
# centralized structured logging to improve observability, debugging,
# and production readiness.
#
# Previously, the pipeline used scattered print() statements, which
# worked during development but were not scalable for production.
# By moving logging setup into logging_utils.py, I created a reusable,
# centralized logger that:
#
# - Standardizes log formatting across the pipeline
# - Writes logs both to console and file
# - Improves troubleshooting when failures occur
# - Makes the ETL process easier to monitor in production
#
# This also keeps main.py cleaner and aligns with modular design
# principles by separating observability concerns from business logic.
#
# DESIGN PRINCIPLE:
# 👉 "logging_utils.py manages observability, not ETL business logic."
# ============================================================

