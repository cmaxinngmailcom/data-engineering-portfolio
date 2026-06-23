# ============================================================
# AIRFLOW DAG
# ============================================================
#
# PURPOSE
# -------
# Execute the Athletes ETL pipeline.
#
# WORKFLOW
# --------
# Airflow Scheduler
#        ↓
# Airflow DAG
#        ↓
# BashOperator
#        ↓
# Python ETL (main.py)
#        ↓
# SQL Server
#        ↓
# dbo.Athletes
#
# DAG ID
# ------
# athletes_etl_pipeline
#
# HOW TO RUN
# ----------
# 1. Open Airflow UI
#    http://localhost:8080
#
# 2. Enable DAG
#
# 3. Click "Trigger DAG"
#
# 4. Review task logs
#
# EXPECTED RESULT
# ---------------
# - Reads bios.csv
# - Applies validation rules
# - Removes duplicates
# - Writes rejects
# - Loads dbo.Athletes
# - Writes ETL audit record
#
# ============================================================

from datetime import datetime
from datetime import timedelta

# Airflow DAG object
from airflow import DAG

# Executes shell commands
from airflow.operators.bash import BashOperator


# ============================================================
# DEFAULT TASK SETTINGS
# ============================================================
#
# owner
#     Logical owner of the DAG
#
# retries
#     Number of retries if task fails
#
# retry_delay
#     Wait time between retries
#
# ============================================================

default_args = {

    "owner": "claude",

    # Retry once if ETL fails
    "retries": 1,

    # Wait 5 minutes before retry
    "retry_delay": timedelta(minutes=5)
}


# ============================================================
# DAG DEFINITION
# ============================================================
#
# dag_id
#     Unique Airflow DAG name
#
# description
#     Shown in Airflow UI
#
# start_date
#     Required by Airflow
#
# schedule_interval
#     None = manual execution only
#
# catchup
#     False = don't run historical executions
#
# tags
#     Used for filtering in Airflow UI
#
# ============================================================

with DAG(

    dag_id="athletes_etl_pipeline",

    default_args=default_args,

    description="Run Athletes ETL pipeline",

    start_date=datetime(2026, 1, 1),

    schedule_interval=None,

    catchup=False,

    tags=[
        "portfolio",
        "etl",
        "sqlserver",
        "airflow",
        "python"
    ]

) as dag:

    # ========================================================
    # TASK 1
    # ========================================================
    #
    # Execute Python ETL pipeline.
    #
    # Equivalent local command:
    #
    # python -m athletes_etl.main
    #
    # ========================================================

    run_athletes_etl = BashOperator(

        task_id="run_athletes_etl",

        bash_command="""
        cd /opt/airflow &&
        python -m athletes_etl.main
        """
    )


# ============================================================
# END OF DAG
# ============================================================
#
# Current workflow:
#
# run_athletes_etl
#
# Future enhancements:
#
# extract_task
#      ↓
# validate_task
#      ↓
# load_task
#      ↓
# audit_task
#
# ============================================================