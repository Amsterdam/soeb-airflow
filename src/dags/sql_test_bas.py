from pathlib import Path
from typing import Final

from airflow import DAG
from common import MessageOperator, default_args, quote_string
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_operator import PostgresOnAzureOperator
from sql.temp_table import CREATE_BAG_PND


DAG_ID: Final = "sql_test_bas"

# The local database connection.
# This secret must exists in KV: `airflow-connections-soeb-postgres`
# with the connection string present with protocol `postgresql://`
# SOEB_DB_CONN_STRING: Final = Connection.get_connection_from_secrets(conn_id ="soeb_postgres" )
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
    description="sql test voor manipuleren data", # zichtbaar als tooltip 
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
    schedule_interval="0 13 * * *", 
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        )

    #  .sql file aanroepen en data verrijken, logica toevoegen
    # 2. Execute SQL
    sql_task = PostgresOnAzureOperator(
        postgres_conn_id="soeb_postgres",
        task_id="set_datatype_date",
        sql="""
        ALTER TABLE public.gas_hoge_druk
        drop column if exists lengte_berekend,
        add column lengte_berekend float;
    
        update public.gas_hoge_druk
        set lengte_berekend = round(ST_Length(geometry)::numeric,3)
        where 1=1;
                
        """
    )
   
    
# FLOW.
    (
    slack_at_start
    >> sql_task
    )

dag.doc_md = """
    #### DAG summary
    Aanroepen van een .sql file binnen airflow
    """