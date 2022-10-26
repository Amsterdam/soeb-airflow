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
DAG_ID: Final = "rioolnetwerk"
variables: dict[str, str] = Variable.get("rioolnetwerk", deserialize_json=True)
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