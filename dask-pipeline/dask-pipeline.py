import dask.dataframe as dd
import dask

import re
import sys

#importers
from pm4py import convert_to_event_log, convert_to_dataframe, format_dataframe

# Miners
from pm4py import convert_to_petri_net, discover_dfg as dfg_discovery, serialize, deserialize
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.correlation_mining import algorithm as correlation_miner
from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery


# Evaluators
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness_evaluator
from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator

from utils import graph_driver, helper

def getDFGQueries(dfg):
    listOfQueries = []
    queryTemplate = """
        MERGE (p:Activity {{name: '{parent}'}})
        MERGE (c:Activity {{name: '{child}'}})
        MERGE (p)-[r:PRODUCES]->(c)
        ON CREATE SET r.frequency={frequency}
        ON MATCH SET r.frequency=r.frequency+{frequency}
    """
    for parent, child in dfg:
        frequency = dfg[(parent, child)]
        template = queryTemplate.format(parent=parent, child=child, frequency=frequency)
        listOfQueries.append(template)
    return listOfQueries

def saveDFG(dfg):
    dfgResult = dfg_discovery(dfg)
    dfgQuery = getDFGQueries(dfgResult.graph)
    neo4jConnection = graph_driver(uri_scheme="neo4j",host="neo4j", password="123456")
    result = neo4jConnection.run_bulk_query(dfgQuery)
    return dfgResult

if __name__ == '__main__':
    sys.setrecursionlimit(30000)
    columnTypes = {
        'case:IDofConceptCase': 'string',
        'case:Includes_subCases': 'string',
        'case:Responsible_actor': 'string',
        'case:caseProcedure': 'string',
        'case:concept:name': 'int64',
        'dateStop': 'string'
    }
    df = dd.read_csv('../data/BPIC15_1.csv', dtype=columnTypes)
    for column in df.columns:
        if re.search("[Dd]ate.*|time.*", column):
            df[column] = dask.dataframe.to_datetime(df[column], utc=True)

    indexed_df = df.set_index('case:concept:name', drop=False, sorted=True)
    indexed_df.index = indexed_df.index.rename('caseId')
    indexed_df = indexed_df.repartition(npartitions=4)

    lazyDFG = indexed_df.map_partitions(saveDFG(indexed_df)).to_delayed()
    dfg_output = helper.getComputeTime(*lazyDFG, scheduler='processes', meta=[])