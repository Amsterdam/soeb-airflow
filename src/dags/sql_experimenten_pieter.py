from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from swift_operator import SwiftOperator
from sqlalchemy.engine.url import make_url
from postgres_on_azure_operator import PostgresOnAzureOperator

# Define variables
DAG_ID: Final = "sql_experimenten_pieter"
TABELNAAM = "public.test2_pds"
# The local database connection.
# This secret must exists in KV: `airflow-connections-soeb-postgres`
# with the connection string present with protocol `postgresql://`
#SOEB_DB_CONN_STRING: Final = Connection.get_connection_from_secrets(conn_id ="soeb_postgres" )
#SOEB_DB_CONN_STRING: Final = Variable.get("soeb_postgres")
#dsn_url = make_url(SOEB_DB_CONN_STRING)
SOEB_DB_CONN_STRING: Final = Variable.get("soeb_postgres")
dsn_url = make_url(SOEB_DB_CONN_STRING)
SOEB_HOST: Final = dsn_url.host
SOEB_PORT: Final = 5432
SOEB_USER: Final = dsn_url.username
SOEB_PASSWD: Final = dsn_url.password
SOEB_DBNAME: Final = dsn_url.database

# DAG definition
with DAG(
    DAG_ID,
    description="sql test pieter",
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
    sql_task1 = PostgresOnAzureOperator(
        postgres_conn_id="soeb_postgres",
        task_id=f"Create_table",
        sql=f"""
        DROP TABLE IF EXISTS "{TABELNAAM}";
        CREATE TABLE IF NOT EXISTS "{TABELNAAM}"
        (
            teller integer NOT NULL,
            surname character varying(30) COLLATE pg_catalog."default",
            name character varying(30) COLLATE pg_catalog."default",
            address character varying(50) COLLATE pg_catalog."default",
            place character varying(50) COLLATE pg_catalog."default",
            zip character varying(7) COLLATE pg_catalog."default"
        )
        """
    )


# FLOW
    (
    slack_at_start
    >>  sql_task1
    )

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
