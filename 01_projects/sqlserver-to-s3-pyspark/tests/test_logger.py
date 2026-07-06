# ============================================================
# TEST LOGGER MODULE (test_logger.py)
# ============================================================
# PURPOSE:
# Test the application's logger configuration.
#
# FIRST GOAL:
# Verify that logger.py correctly creates and configures
# the project logger.
#
# WHAT IS TESTED:
# - Logger object is created
# - Logger level is INFO
# - Console handler exists
# - File handler exists
# - Log file is created
#
# WHY THIS TEST?
# Logging is critical in production ETL pipelines for
# monitoring, troubleshooting, and auditing.
#
# ============================================================

import logging
from pathlib import Path

from sqlserver_to_s3.logger import get_logger


# ============================================================
# TEST: LOGGER CREATION
# ============================================================

def test_get_logger():

    logger = get_logger()

    assert isinstance(logger, logging.Logger)


# ============================================================
# TEST: LOGGER LEVEL
# ============================================================

def test_logger_level():

    logger = get_logger()

    assert logger.level == logging.INFO


# ============================================================
# TEST: LOGGER HAS CONSOLE HANDLER
# ============================================================

def test_console_handler_exists():

    logger = get_logger()

    console_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    assert len(console_handlers) > 0


# ============================================================
# TEST: LOGGER HAS FILE HANDLER
# ============================================================

def test_file_handler_exists():

    logger = get_logger()

    file_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.FileHandler)
    ]

    assert len(file_handlers) > 0


# ============================================================
# TEST: LOG FILE IS CREATED
# ============================================================

def test_log_file_created():

    logger = get_logger()

    logger.info("Testing logger...")

    log_file = Path("logs/pipeline.log")

    assert log_file.exists()


# ============================================================
# INTERVIEW QUESTION & ANSWER
# ============================================================
#
# Q:
# Why unit test a logger?
#
# A:
#
# Logging is an essential part of production ETL pipelines.
#
# A logger test verifies:
#
# • Logger initialization
# • Correct log level
# • Console logging
# • File logging
#
# Testing the logger ensures that operational information
# will be captured during pipeline execution, making
# troubleshooting and monitoring easier.
#
# ============================================================