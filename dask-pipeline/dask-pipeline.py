import dask
import os
import time

import dask.dataframe as dd
from dask.dataframe.utils import make_meta
from dask.distributed import Client, LocalCluster, get_worker

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

from tqdm import tqdm

from config.credentials import getNeo4jCredentials
from utils.helper import setLinks
from utils.graph import addActivity

if __name__ == '__main__':
    columnTypes = {
        'case:IDofConceptCase': 'string',
        'case:Includes_subCases': 'string',
        'case:Responsible_actor': 'string',
        'case:caseProcedure': 'string',
        'dateStop': 'string'
    }
    client = Client()
    df = dd.read_csv('data/BPIC15_1.csv', dtype=columnTypes)
    df['successor'] = ''
    df['predecessor'] = ''
    df = df.sort_values(by='time:timestamp').groupby('case:concept:name').apply(setLinks, meta=df)

    creds = getNeo4jCredentials()
    driver = GraphDatabase.driver(creds.get('host'), auth=(creds.get('user'), creds.get('password')))

    result = df['activityNameEN'].unique()


    def connect_worker_db():
        creds = getNeo4jCredentials()
        worker = get_worker()
        worker.driver = GraphDatabase.driver(creds.get('host'), auth=(creds.get('user'), creds.get('password')))


    client.register_worker_callbacks(connect_worker_db)

    def experiment():
        driver = get_worker().driver
        with driver.session() as session:
            result.apply(lambda activityName: addActivity(session, activityName),
                         meta=('activityNameEN', 'object')).compute()
        session.close()
        driver.close()
        return 'done'


    result = client.submit(experiment)
    result.result()
    # # needs to be refactored and run in dask instead
    # with driver.session() as session:
    #     result.apply(lambda activityName: addActivity(session, activityName), meta=('activityNameEN', 'object')).compute()
    #     # for activityName in result.iteritems():
    #     #     session.write_transaction(addActivity, activityName[1])
    # session.close()
    #
    # driver.close()