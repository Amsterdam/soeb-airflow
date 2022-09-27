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


# Schema: https://schemas.data.amsterdam.nl/datasets/rioolnetwerk/dataset
DAG_ID: Final = "rioolnetwerk_khalid"
variables: dict[str, str] = Variable.get("rioolnetwerk_khalid", deserialize_json=True)
file_to_download: dict[str, list] = variables["files_to_download"]["gpkg_file"]
# files_to_download: dict[str, list] = variables["files_to_download"]
#file_to_download: str = files_to_download["gpkg_file"]

# The temporary directory that will be used to store the downloaded file(s)
# to in the pod.
TMP_DIR: Final = f"{SHARED_DIR}/{DAG_ID}"

# The name of the file to download
DOWNLOAD_PATH_LOC: Final = f"{TMP_DIR}/{file_to_download}"

# The local database connection.
# This secret must exists in KV: `airflow-connections-soeb-postgres`
# with the connection string present with protocol `postgresql://`
#SOEB_DB_CONN_STRING: Final = Connection.get_connection_from_secrets(conn_id ="soeb_postgres" )
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
    description="rioolnetwerk test van khalid",
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
    download_data = SwiftOperator(
            task_id=f"download_{file_to_download}",
            swift_conn_id="OBJECTSTORE_WATERNET",
            container="production",
            object_id=file_to_download,
            output_path=f"{DOWNLOAD_PATH_LOC}",
        )


    # 4. Import data to local database
    import_data_local_db = BashOperator(
            task_id="import_data_into_local_db",
            bash_command="ogr2ogr -overwrite -f 'PostgreSQL' "
            f"'PG:host={SOEB_HOST} dbname={SOEB_DBNAME} user={SOEB_USER} \
                password={SOEB_PASSWD} port={SOEB_PORT} sslmode=require' "
            f"{DOWNLOAD_PATH_LOC} "
            "-t_srs EPSG:28992 -s_srs EPSG:28992 "
            "-lco GEOMETRY_NAME=geometry "
            "-lco FID=id"
            "-lco SCHEMA=stg",
        )


# FLOW
slack_at_start >> mkdir >> download_data >> import_data_local_db

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
