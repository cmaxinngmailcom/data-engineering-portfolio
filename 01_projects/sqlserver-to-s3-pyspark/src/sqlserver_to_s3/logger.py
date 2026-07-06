# ============================================================
# LOGGER MODULE (logger.py)
# ============================================================
# PURPOSE:
# This module creates and configures the project's logger.
#
# FIRST GOAL (learning phase):
# - Move logger configuration out of main.py.
#
# WHAT SHOULD LIVE HERE:
# - Create logger
# - Console logging
# - File logging
# - Log formatting
# - Log levels
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How should the application log information?"
#
# BEST PRACTICES:
# - Configure logger only once
# - Avoid duplicate handlers
# - Log to both console and file
# - Automatically create the logs folder
# - Use INFO as the default log level
#
# WHAT SHOULD NOT BE HERE:
# - ETL logic
# - SQL queries
# - Data validation
# - CSV extraction
#
# END RESULT:
#
#     from sqlserver_to_s3.logger import get_logger
#
#     logger = get_logger(__name__)
#
# ============================================================

import logging
import os
import sys


# ============================================================
# CREATE LOGGER
# ============================================================

def get_logger(name: str = "sqlserver_to_s3") -> logging.Logger:
    """
    Create and return the application logger.

    Logs are written to:
        - Console
        - logs/pipeline.log
    """

    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s"
    )

    # --------------------------------------------------------
    # Create logs folder automatically
    # --------------------------------------------------------

    os.makedirs("logs", exist_ok=True)

    # --------------------------------------------------------
    # Console Handler
    # --------------------------------------------------------

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # --------------------------------------------------------
    # File Handler
    # --------------------------------------------------------

    file_handler = logging.FileHandler(
        "logs/pipeline.log",
        mode="a",
        encoding="utf-8"
    )

    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why create a separate logger.py module?
#
# A:
#
# Separating logger configuration from business logic keeps
# the application modular, reusable, and easier to maintain.
#
# Every module shares the same logger configuration instead of
# configuring logging multiple times.
#
# Logging to both the console and a file provides:
#
# • Real-time monitoring during execution
# • Historical logs for troubleshooting
# • Consistent log formatting
#
# This follows the Single Responsibility Principle (SRP):
#
# logger.py
#     → Creates and configures the logger
#
# main.py
#     → Uses the logger
#
# extract.py
#     → Uses the logger
#
# transform.py
#     → Uses the logger
#
# load.py
#     → Uses the logger
#
# This design is commonly used in enterprise ETL pipelines.
#
# ============================================================