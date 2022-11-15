from pathlib import Path
from typing import Final

import sqlparse

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from common import MessageOperator, default_args, quote_string
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_operator import PostgresOnAzureOperator

# Schema: https://schemas.data.amsterdam.nl/datasets/rioolnetwerk/dataset
DAG_ID: Final = "sql_test"
SQL_DIR = Path("dags/repo/src/dags/sql")

def readSql(sqlFileName: str):
    sql_path = Path(f"{SQL_DIR}/{sqlFileName}.sql")

    # Read the SQL script into memory
    with open(sql_path, "r") as file:
        raw = file.read()

    # Split the SQL script into individual queries
    _queries = sqlparse.split(raw)

    # Build a TaskGroup with sub-tasks for each parsed query.
    with TaskGroup(sqlFileName) as _t:
        _qt_prev = None
        for i, _q in enumerate(_queries):
            # Set the operator arguments (based on config)
            op_args = {
                "sql": _q,
                "postgres_conn_id": "soeb_postgres",
                "doc": str(_queries[i]),
                "autocommit": True,
            }
            _qt = PostgresOnAzureOperator(task_id=f"{sqlFileName}_{i}", **op_args)
            if _qt_prev is not None:
                _qt_prev >> _qt
            _qt_prev = _qt
    return _t

# DAG definition
with DAG(
    DAG_ID,
    description="Use a sql file from within the repository",
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Execute SQL
    sql_task = readSql("dummy")

# FLOW
slack_at_start >> sql_task 

dag.doc_md = """
    #### DAG summary
    This DAG contains ...
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/rioolnetwerk.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/rioolnetwerk.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=rioolnetwerk/rioolknopen&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=rioolnetwerk/rioolleidingen&x=106434&y=488995&radius=10
"""
