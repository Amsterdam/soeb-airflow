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
creadirs: list = ['/tmp/work','/tmp/work/old','/tmp/work/new']


def fcreadirs(dirnames: list):
    for dir in dirnames:
        # os.makedirs(dir, 0o666)
        os.makedirs(dir)
     
def fremovedirs(dirname: str):
    # shutil.rmtree(dir)
    print(dirname)

def fcreatefile(dirname: str, filename: str):
    file=os.path.join(dirname, filename)
    with open(file, 'w') as fileinst:
        fileinst.write('Create a new text file!')
        fileinst.close()
    
@task(task_id="print_the_context")
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

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
    run_this = print_context()


# FLOW
slack_at_start >> run_this

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
