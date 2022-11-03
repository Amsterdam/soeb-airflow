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

DAG_ID: Final = "rioolnetwerk_pieter"
creadirs: dict= {1: '/tmp/work',2: '/tmp/work/old',3: '/tmp/work/new'}

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
            bash_command=f"mkdir -p {creadirs[dirno]}",
        )
    for dirno in range(1,4)
    ]


# FLOW
    (
    slack_at_start
    >> make_temp_dirs 
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
