#import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from swift_operator import SwiftOperator


# Note: Rename DAG_ID to the correct dataset name. If you want to use the correct DB user in de ref DB.
# Schema: https://schemas.data.amsterdam.nl/datasets/rioolnetwerk/dataset
DAG_ID: Final = "rioolnetwerk"
TABLE_DATASET_NAME: Final = "rioolnetwerk"
variables_covid19: dict = Variable.get("rioolnetwerk", deserialize_json=True)
files_to_download: dict = variables_covid19["files_to_download"]

TMP_DIR: Final = f"{SHARED_DIR}/{DAG_ID}"
DB_LOCAL_CONN_STRING: Final = BaseHook.get_connection("soeb_postgres")
DATA_FILE: Final = f"{TMP_DIR}/Waternet_Assets_Levering.gpkg"


with DAG(
    DAG_ID,
    description="rioolnetwerk base test",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_DIR))

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            swift_conn_id="OBJECTSTORE_WATERNET",
            container=f"production/waternet/{file}",
            object_id=file,
            output_path=f"{TMP_DIR}/{file}",
        )
        for file in files_to_download
    ]

    # 5. Dummy operator acts as an interface between parallel tasks to
    # another parallel tasks with different number of lanes
    # (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # Import data to local database
    import_data_local_db = BashOperator(
            task_id="import_data_into_local_db",
            bash_command="ogr2ogr -overwrite -f 'PostgreSQL' "
            f"'PG:host={DB_LOCAL_CONN_STRING.host} dbname={DB_LOCAL_CONN_STRING.schema} user={DB_LOCAL_CONN_STRING.login} \
                password={DB_LOCAL_CONN_STRING.password} port={DB_LOCAL_CONN_STRING.port} sslmode=require' "
            f"{DATA_FILE} "
            "-t_srs EPSG:28992 -s_srs EPSG:28992 "
            "-lco GEOMETRY_NAME=geometry "
            "-nlt PROMOTE_TO_MULTI "
            "-lco FID=id",
        )


# FLOW
slack_at_start >> mkdir >> download_data

for download in zip(download_data):

    download >> Interface

Interface >> import_data_local_db


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
