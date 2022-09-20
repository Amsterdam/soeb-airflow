# deze operator altijd meenemen (top level) operators zoals (= gelijk aan /groter dan enz. )
import operator
# re = regular expressions
import re
# requests - wordt gebruikt om allerlei soorten berichten te verzenden naar een http server
import requests
# 
from functools import partial

# wordt gebruikt om variablen vast te zetten (typing final = niet meer aan te passsen)
from typing import Final
# Zonder DAG gen Geen DAG (dus mee)
from airflow import DAG
# bv om bv. secrets binnen te harken
from airflow.models import Variable
# Om bash te gebruiken 
from airflow.operators.bash import BashOperator
# dummy wordt gebruikt om paralle tasks te configureren (stappen in dags)
from airflow.operators.dummy import DummyOperator
# voor stukje python voor bepaalde task 
from airflow.operators.python import PythonOperator
# common / shared dir : voor tijdelijk opslag - message: voor slack - bv airflow-soeb kanaal - def args: een set van config (altijd meenemen) - quote sring (altijd meenemen)
from common import SHARED_DIR, MessageOperator, default_args, quote_string
# common .db : def temp - nog even uitzoekem of tijdelijk opslaan nodig is / pg_params : postgres params
from common.db import define_temp_db_schema, pg_params
# zoekt in aiflow-dataservices naar voorgedefineerde sql statements..
from common.sql import SQL_DROP_TABLE
# mag niet weg maar gaat om logging
from contact_point.callbacks import get_contact_point_on_failure_callback
# ogr2ogr operator (duidelijk)
from ogr2ogr_operator import Ogr2OgrOperator
# checks zoals srid enz...(ook voorgefineerd te vinden op ?)
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
# behouden want azure
from postgres_on_azure_operator import PostgresOnAzureOperator
# wordt gebrukt om temp tabellen te copieren naar ander schema ofzo
from postgres_table_copy_operator import PostgresTableCopyOperator
# nodig voor ams schema - provenance = metadata oorsprong van de data
from provenance_rename_operator import ProvenanceRenameOperator
# ophalen dags sql (hergebruik) lees geen hardcoded .sql in dags
#from sql.precariobelasting_add import ADD_GEBIED_COLUMN, ADD_TITLE, RENAME_DATAVALUE_GEBIED
# wordt gebruikt om data op te halen (objectstore?)
from swift_operator import SwiftOperator
from common.path import mk_dir
from pathlib import Path


dag_id = "rioolnetwer"
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
tmp_database_schema: str = define_temp_db_schema(dataset_name=dag_id)
variables: dict = Variable.get(dag_id, deserialize_json=True)
files_to_download = variables["files_to_download"]
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=dag_id)


with DAG(
    dag_id,
    description="Rioolnetwerken aangeleverd door Waternet",
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
    ) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    # NOTE kan ook met bashoperator:
    # make_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")
    make_temp_dir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file_name}",
            swift_conn_id="objectstore-waternet", # laatste 2 namen van key-vault-string gebruiken (airflow-connections-objectstore-waternet)
            container="production", # map in de objectstore
            object_id=url,
            output_path=f"{tmp_dir}/{url}",
        )
        #for file_name, url in data_endpoints.items() # check vars.yml
        # op meerdere plekken zie ik .values() vs .items() staan...ff checken
        for file_name, url in files_to_download.items() # veranderd naar .items
    ]

     
'''    # 4. Import .gpkg to Postgresql
    # NOTE: ogr2ogr demands the PK is of type integer.
    # ook hier kom ik BashOperator tegen die ogr2ogr gebruikt...
    import_data = [
        Ogr2OgrOperator(
            task_id="import_data_{file_name}",
            target_table_name=f"{dag_id}_{file_name}_new",
            db_schema=tmp_database_schema,
            input_file=f"{tmp_dir}/{dag_id}",
            # f"-nln {file_name}",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            auto_detect_type="YES",
            geometry_name="geometry",
            fid="fid",
            mode="PostgreSQL",
        )
        for file_name, url in files_to_download.items()
    ]
    # FLOW.
'''    (
    slack_at_start
    >> make_temp_dir 
    >> download_data
#    >> import_data
    )

dag.doc_md = """
    #### DAG summary
    This DAG contains Rioolnetwerken from Waternet (khalid)
    """