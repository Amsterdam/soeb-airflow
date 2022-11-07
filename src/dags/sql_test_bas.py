from pathlib import Path
from typing import Final

from airflow import DAG
from common import MessageOperator, default_args, quote_string
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_on_azure_operator import PostgresOnAzureOperator

DAG_ID: Final = "sql_test_bas"

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
        task_id="calc length from geometry",
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
    Aanroepen van een .sql statement binnen airflow
    """