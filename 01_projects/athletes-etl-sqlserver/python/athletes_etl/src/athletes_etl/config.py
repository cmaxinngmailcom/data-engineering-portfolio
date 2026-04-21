# ============================================================
# CONFIG MODULE (config.py)
# ============================================================
# PURPOSE:
# This module is responsible for loading and managing configuration
# for the ETL pipeline in a safe and reusable way.
#
# FIRST GOAL (learning phase):
# - Move configuration-related logic out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - load_config() function → reads YAML file (e.g., dev.yaml)
# - cfg = load_config("dev") → returns config as a dictionary
# - Logic to read values from YAML (pipeline, extract, sqlserver, etc.)
# - Helper functions to resolve paths (e.g., CSV path, project root)
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How do I load and return configuration safely?"
#
# BEST PRACTICES:
# - Do NOT hardcode values in code → always read from YAML
# - Do NOT store secrets (like passwords) in YAML
# - Use environment variables for sensitive data
# - Ensure paths work regardless of where the script is executed
#
# END RESULT:
# main.py should NOT deal with YAML directly.
# Instead, it should call functions from this module like:
#
#     from athletes_etl.config import load_config
#     cfg = load_config("dev")
#
# ============================================================

# ============================================================
# CONFIG MODULE
# ============================================================
# Handles:
# - Loading YAML config (dev.yaml)
# - Returning config as dictionary
# ============================================================

from pathlib import Path
import yaml


def load_config(env: str = "dev") -> dict:
    """
    Load configs/{env}.yaml and return as dictionary.

    Example:
        cfg = load_config("dev")
    """
    project_root = Path(__file__).resolve().parents[2]
    cfg_path = project_root / "configs" / f"{env}.yaml"

    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
    # ✅ Resolve CSV path (handles relative + absolute)
    csv_path = cfg["extract"]["csv_path"]
    csv_path = Path(csv_path)

    if not csv_path.is_absolute():
        csv_path = project_root / csv_path

    cfg["extract"]["csv_path"] = str(csv_path)

    return cfg

import os

# Add ENV password handling (clean)

def get_sql_password(cfg: dict) -> str:
    """
    Get SQL password from environment variable.
    """
    pwd_env = cfg["sqlserver"]["password_env"]
    password = os.getenv(pwd_env)

    if not password:
        raise RuntimeError(f"Missing environment variable: {pwd_env}")

    return password

# 🎯 What you just achieved

# You now have:

# ✅ Clean separation of concerns
# ✅ No YAML logic in main.py
# ✅ Safe path handling
# ✅ Secure password handling
# ✅ Production-style config design

# 🧠 Interview gold (your answer)

# If asked: “How do you manage configuration?”

# Say:

# I externalize all configuration in YAML files and load them through a dedicated config module.
# I also resolve file paths dynamically and retrieve sensitive data like passwords from environment variables to keep the system secure and portable.