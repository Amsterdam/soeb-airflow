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

import datetime

# Define variables
DAG_ID: Final = "rioolnetwerk_pieter"
creadirs: dict = {1: '/tmp/work',2: '/tmp/work/old',3: '/tmp/work/new'}
osfilelocation = 'production'
osfilewaternet = 'Waternet_Assets_Levering.gpkg'
oskdrvfilelocation = 'geopackages'
weeksdict: dict = {}
today = datetime.datetime.now()
weekrange = [0,5,6,7,8]
for weekno in weekrange:
   wtoday = (today - datetime.timedelta(weeks=weekno)).strftime("%V")
   ytoday = (today - datetime.timedelta(weeks=weekno)).year
   weekfile = 'Waternet_Assets_Levering_'+ str(ytoday) + '_week' + str(wtoday) + '.zip'
   weeksdict[weekno] = weekfile

# DAG definition
with DAG(
    DAG_ID,
    description="rioolnetwerk_pieter",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directories to store files
    make_temp_dirs = [
        BashOperator(
            task_id=f"Make_directory_{dirno}",
            bash_command=f"mkdir -p {creadirs.get(dirno)}",
        )
    for dirno in range(1,4)
    ]

    # Download waternet file from objectstore to work old
    download_waternetfile = SwiftOperator(
        task_id=f"download_{osfilewaternet}",
        swift_conn_id="OBJECTSTORE_WATERNET",
        action_type="download",
        container=f"{osfilelocation}",
        object_id=f"{osfilewaternet}",
        output_path=f"{creadirs.get(3)}/{osfilewaternet}",
    )

    # Zip new waternet file
    zip_new_waternetfile = BashOperator(
        task_id=f"zip_{weeksdict.get(0)}",
        bash_command=f"zip -q {creadirs.get(3)}/{weeksdict.get(0)} {creadirs.get(3)}/{osfilewaternet}" 
    )
    
    # Delete new kdrive object store
    delete_new_waternetfile = SwiftOperator(
        task_id=f"delete_{weeksdict.get(0)}",
        swift_conn_id="OBJECTSTORE_WATERNET",
        action_type="delete",
        # container=f"{oskdrvfilelocation}",
        # objects_to_del=f"{weeksdict.get(0)}",
        # container="geopackages",
        container="production",
        objects_to_del="Waternet_Assets_Levering_2022_week44.zip",
    )

    # delete old files from objectsore
    # remove directorues

# FLOW
    (
    slack_at_start
    >> make_temp_dirs
    >> download_waternetfile
    >> zip_new_waternetfile
    >> delete_new_waternetfile
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
