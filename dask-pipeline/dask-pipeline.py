from config.credentials import getNeo4jCredentials
from utils.helper import setLinks
from utils.graph import addActivity
import dask.dataframe as dd
from neo4j import GraphDatabase

if __name__ == '__main__':
    columnTypes = {
        'case:IDofConceptCase': 'string',
        'case:Includes_subCases': 'string',
        'case:Responsible_actor': 'string',
        'case:caseProcedure': 'string',
        'dateStop': 'string'
    }
    df = dd.read_csv('data/BPIC15_1.csv', dtype=columnTypes)
    df = df.sort_values(by='time:timestamp').groupby('case:concept:name').apply(setLinks)

    creds = getNeo4jCredentials()
    driver = GraphDatabase.driver(creds.get('host'), auth=(creds.get('user'), creds.get('password')))

    with driver.session() as session:
        df['activityNameEN'].unique().apply(lambda activityName: session.write_transaction(addActivity, activityName), meta=('activityNameEN', 'object'))
    session.close()