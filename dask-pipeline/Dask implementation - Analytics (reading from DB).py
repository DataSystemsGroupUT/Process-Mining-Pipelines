import dask.dataframe as dd
from dask.dataframe import from_pandas
from dask.dataframe.utils import make_meta
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
from dask.distributed import Client, LocalCluster, get_worker
import dask

import os
import time
from tqdm import tqdm
import pandas as pd
import re
import gc
import numpy as np


# Miners
from pm4py import serialize, deserialize
from pm4py import discover_dfg as dfg_discovery
from pm4py.discovery import DFG

from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py import discover_petri_net_inductive as inductive_miner


# Evaluators

from pm4py import fitness_token_based_replay as fitness_token_based_replay #fitness
from pm4py import precision_alignments as precision_token_based_replay #precision
# from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator #simplicity
# from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator #generalization
# from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
# from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness_evaluator
# from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
# from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator


# In[ ]:





# In[ ]:


import sys



# In[ ]:


import ctypes

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


# In[ ]:


class graph_driver():
    def __init__(self, uri_scheme='bolt', host='localhost', port='7687', username='neo4j', password='123456'):
        self.uri_scheme = uri_scheme
        self.host = host
        self.port = port
        
        self.username = username
        self.password = password
        
        self.connection_uri = "{uri_scheme}://{host}:{port}".format(uri_scheme=self.uri_scheme, host=self.host, port=self.port)
        self.auth = (self.username, self.password)
        self.driver = GraphDatabase.driver(self.connection_uri, auth=self.auth)
        
    def __del__(self):
        self._close_driver()
    
    def _close_driver(self):
        if self.driver:
            self.driver.close()
    
    def run_single_query(self, query):
        res = None
        with self.driver.session() as session:
            raw_res = session.run(query)
            res = self.format_raw_res(raw_res)
        return res
    
    def run_bulk_query(self, query_list):
        results = []
        with self.driver.session() as session:
            for query in tqdm(query_list):
                raw_res = session.run(query)
                res = self.format_raw_res(raw_res)
                results.append({'query':query, 'result':res})
        return results
    
    def reset_graph(self, db=None):
        return self.run_single_query("MATCH (n) DETACH DELETE n")
    
    def test_connection(self):
        return self.run_single_query("MATCH (n) RETURN COUNT(n) as nodes")
    
    @staticmethod
    def format_raw_res(raw_res):
        res = []
        for r in raw_res:
            res.append(r)
        return res


# In[ ]:


def useExecutionTime(func):
    
    def compute(*args, **kwargs):
        begin = time.time()
        
        result = func(*args, **kwargs)
        
        end = time.time()
        
        return {"result": result, "execution_time": end - begin}
 
    return compute

@useExecutionTime
def getComputeTime(*args, **kwargs):
    return dask.compute(*args, **kwargs)


# In[ ]:


cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit=None)


def run_gc(dask_worker,**kwargs):
    gc.collect()
    return True



# In[ ]:


def transformToDFG(dfgResult):
    result = {}
    for record in dfgResult:
        result[(record["parent"], record["child"])] = record["frequency"]
    
    return result

def transformToStartEndActivity(activities):
    result = {}
    for record in activities:
        result[record['name']] = record["frequency"]
        
    return result


# In[ ]:


def getDFG():
    queries = {
        "dfgQuery": """MATCH result=(p:Activity)-[r:PRODUCES]->(c:Activity) RETURN p.name as parent, c.name as child, r.frequency as frequency""",
        "startEndActivitiesQuery": ["MATCH (a:StartActivity) RETURN a.name as name , a.frequency as frequency", "MATCH (a:EndActivity) RETURN a.name as name , a.frequency as frequency"],
    }
    
    neo4jConnection = graph_driver(uri_scheme="neo4j",host="neo4j", password="123456")
    
    dfgResult = neo4jConnection.run_single_query(queries['dfgQuery'])
    startEndActivitiesResult = neo4jConnection.run_bulk_query(queries['startEndActivitiesQuery'])
    return [transformToDFG(dfgResult), transformToStartEndActivity(startEndActivitiesResult[0]["result"]), transformToStartEndActivity(startEndActivitiesResult[1]["result"])]
    


@useExecutionTime
def getMinerResult(dfg, miner, threshold = 0.5):
    result = {}
    if miner == 'heuristic_miner':
        net, im, fm = heuristics_miner.apply_dfg(dfg['dfg'], parameters={heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: threshold})
    elif miner == 'inductive_miner':
        net, im, fm = inductive_miner(dfg['dfgObj'])
    elif miner == 'alpha_miner':
        net, im, fm = alpha_miner.apply_dfg(dfg['dfg'])
    
    result[miner] = serialize(net, im, fm)
    
    return result
    
def setLazyMiners(dfg):
    lazyList = []
    miners = [
        'heuristic_miner',
        'inductive_miner',
#         'alpha_miner'
    ]
    for miner in miners:
        task = dask.delayed(getMinerResult)(dfg, miner)
        lazyList.append(task)
    
    return lazyList


# In[ ]:


@useExecutionTime
def getMetrics(log, miner, metric, net, im, fm):
    try:
        result = {
            miner: {
                metric: 0
            }
        }
        if metric == 'fitness':
            result[miner][metric] = fitness_token_based_replay(log, net, im, fm)
        elif metric == 'simplicity':
            result[miner][metric] = simplicity_evaluator.test(net,,,
        elif metric == 'precision':
            result[miner][metric] = precision_token_based_replay(log, net, im, fm,  activity_key='concept:name', case_id_key='case:concept:name', timestamp_key='time:timestamp')
        elif metric == 'generalization':
            result[miner][metric] = generalization_evaluator.test(log, net, im, fm)

        return result
    except Exception as e:
        return {miner: {metric: {"error": e}}}

def setLazyMetrics(log, miners):
    lazyList = []
    metrics = [
#         'fitness',
#         'simplicity',
        'precision',
#         'generalization'
    ]
    
    for metric in metrics:
        for miner in miners:
            algorithm = list(miner['result'].keys())[0]
            net, im, fm = deserialize(miner['result'][algorithm])
            task = getMetrics(log, algorithm, metric, net, im, fm)
            lazyList.append(task)
    
    return lazyList



if __name__ == '__main__':

    dask.config.set({'distributed.scheduler.active-memory-manager.start': True})
    sys.setrecursionlimit(30000)

    client = Client(cluster)
    client

    # Register the GC function as a plugin
    # client.register_worker_plugin(run_gc, "my_gc_plugin")
    # client.register_worker_plugin(trim_memory, "my_trim_plugin")
    #
    # # In[ ]:
    #
    # cluster.adapt(minimum=1, maximum=6)
    #
    # # In[ ]:
    #
    # columnTypes = {
    #     'case:IDofConceptCase': 'string',
    #     'case:Includes_subCases': 'string',
    #     'case:Responsible_actor': 'string',
    #     'case:caseProcedure': 'string',
    #     'case:concept:name': 'int64',
    #     'dueDate': 'string',
    #     'case:termName': 'string',
    #     'dateStop': 'string',
    #     'case:endDate': 'object',
    #     'case:endDatePlanned': 'object',
    #     'case:parts': 'object'
    # }
    #
    # # list of file paths to be loaded
    # file_paths = ['BPIC15_1.csv']
    #
    # # load the first file as a Dask dataframe
    # df = dd.read_csv(file_paths[0], dtype=columnTypes)
    #
    # # iterate over the remaining files
    # for file_path in file_paths[1:]:
    #     # usecols parameter to load only the columns that are present in both dataframes
    #     df_temp = dd.read_csv(file_path)
    #     # concatenate the dataframes along the rows
    #     df = dd.concat([df, dd.read_csv(file_path, dtype=columnTypes)], interleave_partitions=True)
    #
    # # columnTypes = {
    # #     'OfferID': 'string'
    # # }
    #
    # # fileName = 'BPI Challenge 2017'
    # # df = dd.read_csv('{fileName}.csv'.format(fileName=fileName), dtype=columnTypes)
    # for column in df.columns:
    #     if re.search("[Dd]ate.*|time.*", column):
    #         df[column] = dask.dataframe.to_datetime(df[column], utc=True)
    #
    # # df['case:concept:name'] = df['case:concept:name'].replace(to_replace="Application_", value='', regex=True)
    # df['case:concept:name'] = df['case:concept:name'].astype({'case:concept:name': 'int64'})
    #
    # df = df.repartition(npartitions=1)
    #
    # indexed_df = df.set_index('case:concept:name', drop=False, sorted=True)
    # indexed_df['case:concept:name'] = indexed_df['case:concept:name'].astype({'case:concept:name': 'string'})
    #
    # indexed_df.index = indexed_df.index.rename('caseId')
    # indexed_df = indexed_df.repartition(npartitions=4)
    #
    # dfg, start, end = getDFG()
    # dfgObj = DFG(dfg, start_activities=start, end_activities=end)
    #
    # lazyMiners = setLazyMiners({"dfgObj": dfgObj, "dfg": dfg})
    #
    # lazyMinersResults = dask.compute(*lazyMiners)
    #
    # lazyMetrics = setLazyMetrics(indexed_df, lazyMinersResults)
    #
    # thirdPartition = indexed_df.get_partition(n=0)
    #
    # thirdPartitionResult = setLazyMetrics(thirdPartition.compute(), lazyMinersResults)
    #
    # print(thirdPartitionResult)