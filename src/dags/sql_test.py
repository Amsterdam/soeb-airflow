from pathlib import Path
from typing import Final

from airflow import DAG
from common import MessageOperator, default_args, quote_string
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_operator import PostgresOnAzureOperator

# Schema: https://schemas.data.amsterdam.nl/datasets/rioolnetwerk/dataset
DAG_ID: Final = "sql_test"

# DAG definition
with DAG(
    DAG_ID,
    description="sql_test",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Execute SQL
    sql_task = PostgresOnAzureOperator(
        postgres_conn_id="soeb_postgres",
        task_id="set_datatype_date",
        sql="""
            SELECT 1 FROM spatial_ref_sys;
        """
    )

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
