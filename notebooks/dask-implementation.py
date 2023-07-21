import dask.dataframe as dd
from dask.dataframe import from_pandas
from dask.dataframe.utils import make_meta
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
from dask.distributed import Client, LocalCluster, get_worker
import dask

import os
import time
import timeit
from tqdm import tqdm
import pandas as pd
import re
import gc
import numpy as np
import dill
import sys

# Miners
from pm4py import serialize, deserialize
from pm4py import discover_dfg as dfg_discovery
from pm4py.discovery import DFG

from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py import discover_petri_net_inductive as inductive_miner


# Evaluators
from contribution import fitness_alignment, generalization, precision_alignment
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator #simplicity
from pm4py.objects.petri_net.utils.check_soundness import check_easy_soundness_net_in_fin_marking


class graph_driver():
    def __init__(self, uri_scheme='bolt', host='localhost', port='7687', username='neo4j', password='123456'):
        self.uri_scheme = uri_scheme
        self.host = host
        self.port = port

        self.username = username
        self.password = password

        self.connection_uri = "{uri_scheme}://{host}:{port}".format(uri_scheme=self.uri_scheme, host=self.host,
                                                                    port=self.port)
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
                results.append({'query': query, 'result': res})
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


def getDFG():
    queries = {
        "dfgQuery": """MATCH result=(p:Activity)-[r:PRODUCES]->(c:Activity) RETURN p.name as parent, c.name as child, r.frequency as frequency""",
        "startEndActivitiesQuery": ["MATCH (a:StartActivity) RETURN a.name as name , a.frequency as frequency",
                                    "MATCH (a:EndActivity) RETURN a.name as name , a.frequency as frequency"],
    }

    neo4jConnection = graph_driver(uri_scheme="neo4j", host="neo4j", password="123456")

    dfgResult = neo4jConnection.run_single_query(queries['dfgQuery'])
    startEndActivitiesResult = neo4jConnection.run_bulk_query(queries['startEndActivitiesQuery'])
    return [transformToDFG(dfgResult), transformToStartEndActivity(startEndActivitiesResult[0]["result"]),
            transformToStartEndActivity(startEndActivitiesResult[1]["result"])]


@useExecutionTime
def getMinerResult(dfg, miner, threshold=0.5):
    result = {}
    if miner == 'heuristic_miner':
        net, im, fm = heuristics_miner.apply_dfg(dfg['dfg'], parameters={
            heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: threshold})
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
        'alpha_miner'
    ]
    for miner in miners:
        task = dask.delayed(getMinerResult)(dfg, miner)
        lazyList.append(task)

    return lazyList


@useExecutionTime
def getMetrics(log, miner, metric, net, im, fm):
    sys.setrecursionlimit(3000)
    try:
        result = {
            miner: {
                metric: 0
            }
        }
        if metric == 'fitness':
            result[miner][metric] = fitness_alignment.apply(log, net, im, fm)
        elif metric == 'simplicity':
            result[miner][metric] = simplicity_evaluator.apply(net)
        elif metric == 'precision':
            result[miner][metric] = precision_alignment.apply(log, net, im, fm)
        elif metric == 'generalization':
            result[miner][metric] = generalization.apply(log, net, im, fm)

        return result
    except Exception as e:
        return {miner: {metric: {"error": e}}}


def setLazyMetrics(log, miners):
    lazyList = []
    metrics = [
        'fitness',
        'simplicity',
        'precision',
        'generalization'
    ]

    for metric in metrics:
        for miner in miners:
            algorithm = list(miner['result'].keys())[0]
            net, im, fm = deserialize(miner['result'][algorithm])
            task = getMetrics(log, algorithm, metric, net, im, fm)
            lazyList.append(task)

    return lazyList


@dask.delayed
def aggregate(partitions):
    result = {}
    for partition in partitions:
        for output in partition:
            miner = list(output['result'].keys())[0]
            metric = list(output['result'][miner].keys())[0]
            e_time = output['execution_time']

            result.setdefault(miner, {})
            result[miner].setdefault(metric, None)

            if result[miner][metric] == None:
                result[miner][metric] = output['result'][miner][metric]

            if metric and metric == 'fitness':
                result[miner][metric] = fitness_alignment.aggregate(output['result'][miner][metric],
                                                                    result[miner][metric])
            elif metric and metric == 'precision':
                result[miner][metric] = precision_alignment.aggregate(output['result'][miner][metric],
                                                                      result[miner][metric])
            elif metric and metric == 'generalization':
                result[miner][metric] = generalization.aggregate(
                    [output['result'][miner][metric], result[miner][metric]])

    return result


def compute_metrics(aggregatedMetrics, minersResults):
    results = {}

    getMinerResultByMiner = lambda results, miner: [value for value in lazyMinersResults if
                                                    list(value['result'].keys())[0] == miner].pop()

    for miner, metrics in aggregatedMetrics.items():
        net, im, fm = deserialize(getMinerResultByMiner(minersResults, miner)['result'][miner])
        for metricKey, metricValue in metrics.items():
            results.setdefault(miner, {})
            results[miner].setdefault(metricKey, None)

            if not check_easy_soundness_net_in_fin_marking(net, im, fm) and metricKey == 'precision':
                results[miner][metricKey] = dask.delayed(lambda *args, **kwargs: 'NA')()
                continue
            if metricKey and metricKey == 'fitness':
                results[miner][metricKey] = dask.delayed(fitness_alignment.compute)(metricValue, net=net, im=im, fm=fm)
            elif metricKey and metricKey == 'precision':
                results[miner][metricKey] = dask.delayed(precision_alignment.compute)(**metricValue, net=net, im=im,
                                                                                      fm=fm)
            elif metricKey and metricKey == 'generalization':
                results[miner][metricKey] = dask.delayed(generalization.compute)(**metricValue, net=net)
            elif metricKey and metricKey == 'simplicity':
                results[miner][metricKey] = dask.delayed(simplicity_evaluator.apply)(net)
    # loop over the delayed functions for each miner/metric
    output = {}
    for miner, metrics in results.items():
        output.setdefault(miner, {})
        for metricKey, metricValue in metrics.items():
            output[miner].setdefault(metricKey, {})
            output[miner][metricKey].setdefault('result', 0)
            output[miner][metricKey].setdefault('execution_time', 0)

            start_time = timeit.default_timer()
            output[miner][metricKey]['result'] = dask.compute(results[miner][metricKey])
            end_time = timeit.default_timer()
            output[miner][metricKey]['execution_time'] = end_time - start_time

    return output


def getStatisticalDataFrames(minersResults):
    metricsExecutionTimePerMiner = {}

    for result in minersResults:
        miner = list(result['result'].keys())[0]
        execution_time = result['execution_time']
        metricsExecutionTimePerMiner.setdefault(miner, execution_time)
        metricsExecutionTimePerMiner[miner] = execution_time

    metricsExecutionTimePerMiner['data_set'] = '-'.join(file_paths)

    return pd.DataFrame(metricsExecutionTimePerMiner, index=['execution_time'])

if __name__ == '__main__':
    columnTypes = {
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
        #     'msgCode': 'string',
        #     'msgType': 'string'
    }

    # list of file paths to be loaded

    file_paths = ['BPI_2019']

    # load the first file as a Dask dataframe
    df = dd.read_csv('{}.csv'.format(file_paths[0]), dtype=columnTypes, encoding="ISO-8859-1")

    # iterate over the remaining files
    for file_path in file_paths[1:]:
        # usecols parameter to load only the columns that are present in both dataframes
        df_temp = dd.read_csv('{}.csv'.format(file_path))
        # concatenate the dataframes along the rows
        df = dd.concat([df, dd.read_csv(file_path, dtype=columnTypes)], interleave_partitions=True)

    # columnTypes = {
    #     'OfferID': 'string'
    # }

    # fileName = 'BPI_2019'
    # df = dd.read_csv('{fileName}.csv'.format(fileName=fileName), dtype=columnTypes)
    # df = df.rename(columns={"Incident ID": "case:concept:name", "IncidentActivity_Type": "concept:name", "DateStamp": "time:timestamp"})
    df = df.rename(columns={"case concept:name": "case:concept:name", "event concept:name": "concept:name",
                            "event time:timestamp": "time:timestamp"})
    for column in df.columns:
        if re.search("[Dd]ate.*|time.*", column):
            df[column] = dask.dataframe.to_datetime(df[column], utc=True)

    df['case:concept:name'] = df['case:concept:name'].replace(to_replace="[a-zA-Z]", value='', regex=True)
    df['case:concept:name'] = df['case:concept:name'].astype('int')

    # df = df.repartition(npartitions=1)

    indexed_df = df.set_index('case:concept:name', drop=False, sorted=False)
    indexed_df['case:concept:name'] = indexed_df['case:concept:name'].astype({'case:concept:name': 'string'})

    indexed_df.index = indexed_df.index.rename('caseId')
    indexed_df = indexed_df.repartition(npartitions=4)

    dfg, start, end = getDFG()
    dfgObj = DFG(dfg, start_activities=start, end_activities=end)

    lazyMiners = setLazyMiners({"dfgObj": dfgObj, "dfg": dfg})
    lazyMinersResults = dask.compute(*lazyMiners)
    lazyMetrics = setLazyMetrics(indexed_df, lazyMinersResults)

    mapped_data = indexed_df.map_partitions(setLazyMetrics, lazyMinersResults)
    aggregated_results = aggregate(mapped_data)
    # aggregated_results = aggregated_results.compute()

    r = dask.delayed(compute_metrics)(aggregated_results, lazyMinersResults)
    results = r.compute()

    # print(results)

    savedResult = pd.DataFrame(results)
    miner_execution_time = getStatisticalDataFrames(lazyMinersResults)
    #
    savedResult.to_csv('./results/3 - distributed setup/{}_results.csv'.format('-'.join(file_paths)))
    miner_execution_time.to_csv('./results/3 - distributed setup/{}_miner_execution_time.csv'.format('-'.join(file_paths)))
