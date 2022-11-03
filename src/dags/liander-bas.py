import re

from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from swift_operator import SwiftOperator
from sqlalchemy.engine.url import make_url


DAG_ID: Final = "liander_test"
#variables: dict[str,str] = Variable.get("liander_test", deserialize_json=True)# zie vars.yml
#file_to_download: dict[str, list] = variables["file_to_download"]["zip_file"]
#file_to_download: str = variables["files_to_download"]# zie vars.yml
#files_to_proces: str = variables["files_to_proces"]
#zip_file: str = file_to_download["zip_file"]
# shp_file1: str = file_to_proces["Gas_Hoog"]# let op!: "spaties" in de zip_file niet toegestaan
# shp_file2: str = file_to_proces["Gas_Laag"]

zip_file="Gas_Leidingen.zip"
files_to_proces=["Gas_Hoge_Druk.shp","Gas_Lage_Druk.shp"]

# The temporary directory that will be used to store the downloaded file(s)
TMP_DIR: Final = f"{SHARED_DIR}/{DAG_ID}"

# The location name of the zipfile to download
DOWNLOAD_PATH_LOC: Final = f"{TMP_DIR}/{zip_file}"

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
    description="liander test met .zip en .shp files!", # zichtbaar als tooltip 
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
    # NOTE kan ook met bashoperator:
    # make_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")
    make_temp_dir = mk_dir(Path(TMP_DIR))

    # 3. Download data met SwiftOperator (objectstore)
    download_data = SwiftOperator(
            task_id=f"download_{zip_file}",
            swift_conn_id="OBJECTSTORE_DATARUIMTE", # let op hoofdletters en "-" naar "_": laatste 2 namen van key-vault-string gebruiken (airflow-connections-objectstore-datatuimte)
            container="ondergrond", # map op de objectstore
            object_id=f"liander 14-09-2021/{zip_file}", # verwijzing naar bovenstaande variable
            output_path=f"{DOWNLOAD_PATH_LOC}",
        )
   
    # 4. Extract zip file
    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f"unzip -o {DOWNLOAD_PATH_LOC} -d {TMP_DIR}", #
        )
    # 4a check dir
    check_dir = BashOperator(
        task_id="check_dir",
        bash_command=f"ls {TMP_DIR} -R"
   
        )


    # 5. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    # wordt gebruikt in een for-loop tbv org2ogr met bashoperator
    DAGsplitter = DummyOperator(task_id="interface")


    # 6. (batch) Import data to local database
    # let op! blokhaken ivm for-loop
    import_data_local_db = [
        BashOperator
        (
            task_id=f"import_{A}_into_local_db",
            bash_command="ogr2ogr -overwrite -f 'PostgreSQL' "
            f"'PG:host={SOEB_HOST} dbname={SOEB_DBNAME} user={SOEB_USER} \
            password={SOEB_PASSWD} port={SOEB_PORT} sslmode=require' "
            f"{TMP_DIR}/{A}" # spatie wel/niet meenemen
            "-a_srs EPSG:28992" #let op! -a_srs : asign-flag
            "-lco GEOMETRY_NAME=geometry "
            "-lco FID=id", # -lco : layer creation option
        ) 
    for A in files_to_proces # 
    ]

# FLOW.
    (
    slack_at_start
    >> make_temp_dir 
    >> download_data
    >> extract_zip
    >> check_dir
    >> DAGsplitter 
    >> import_data_local_db
    #for (ingest_data) in zip(import_data_local_db):
    )

dag.doc_md = """
    #### DAG summary
    This test dag met liander gasleidingen (oude data)
    """