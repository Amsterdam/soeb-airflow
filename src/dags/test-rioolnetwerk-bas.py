import requests
import operator

from functools import partial
from typing import Final
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import SHARED_DIR, default_args, MessageOperator, quote_string
from common.db import define_temp_db_schema, pg_params
from contact_point.callbacks import get_contact_point_on_failure_callback
from pathlib import Path
from common.path import mk_dir
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator


dag_id = "rioolnetwerk"
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
            container="waternet", # wat moet hier nou??
            object_id=url,
            output_path=f"{tmp_dir}/{url}",
        )
        #for file_name, url in data_endpoints.items() # check vars.yml
        # op meerdere plekken zie ik .values() vs .items() staan...ff checken
        for file_name, url in files_to_download.items() # veranderd naar .items
    ]

    # 3a Download data SftpOperator
    # todo

    

     
    # 4. Import .gpkg to Postgresql
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
    (
    slack_at_start
    >> make_temp_dir 
    >> download_data
    >> import_data
    )

dag.doc_md = """
    #### DAG summary
    This DAG contains Rioolnetwerken from Waternet
    """