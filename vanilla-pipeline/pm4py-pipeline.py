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

from pm4py import fitness_alignments as fitness_alignments_evaluator #fitness
from pm4py import precision_alignments as precision_alignments_evaluator #precision
# from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator #simplicity
# from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator #generalization
# from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
# from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness_evaluator
# from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
# from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator

def getMinerResult(dfg, miner, threshold=0.5):
    result = {}
    if miner == 'heuristic_miner':
        net, im, fm = heuristics_miner.apply_dfg(dfg['dfg'], parameters={
            heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: threshold})
    elif miner == 'inductive_miner':
        net, im, fm = inductive_miner(dfg['dfgObj'])
    elif miner == 'alpha_miner':
        net, im, fm = alpha_miner.apply_dfg(dfg['dfg'])

    result[miner] = {"net": net, "im": im, "fm": fm}

    return result

def getMetrics(log, miner, net, im, fm):
    try:
        result = {
            miner: {

            }
        }

        metrics = [
            'fitness',
            'simplicity',
            'precision',
            'generalization'
        ]

        for metric in metrics:
            if metric == 'fitness':
                result[miner][metric] = fitness_alignments_evaluator(log, net, im, fm)
            elif metric == 'simplicity':
                pass
                # result[miner][metric] = simplicity_evaluator.apply(net)
            elif metric == 'precision':
                result[miner][metric] = precision_alignments_evaluator(log, net, im, fm)
            elif metric == 'generalization':
                pass
                # result[miner][metric] = generalization_evaluator.apply(log, net, im, fm)

        return result
    except Exception as e:
        return {"error": e}

if __name__ == '__main__':
    dataframe = pd.read_csv('../notebooks/BPI Challenge 2017.csv')
    dataframe['time:timestamp'] = pd.to_datetime(dataframe['time:timestamp'])
    dataframe['concept:name'] = dataframe['concept:name'].astype(str)

    dfg, start_activities, end_activities = dfg_discovery(dataframe)
    dfgObject = DFG(dfg, start_activities=start_activities, end_activities=end_activities)

    # Inductive miner
    inductiveMinerResults = getMinerResult({"dfgObj": dfgObject, "dfg": dfg}, 'inductive_miner')['inductive_miner']
    # Alpha miner
    alphaMinerResults = getMinerResult({"dfgObj": dfgObject, "dfg": dfg}, 'alpha_miner')['alpha_miner']
    # Heuristic miner
    heuristicsMinerResults = getMinerResult({"dfgObj": dfgObject, "dfg": dfg}, 'heuristic_miner')['heuristic_miner']

    #Evaluation Metrics

    # Inductive miner
    inductiveMinerEvaluation = getMetrics(dataframe, 'inductive_miner', inductiveMinerResults['net'], inductiveMinerResults['im'], inductiveMinerResults['fm'])
    # Alpha miner
    alphaMinerEvaluation = getMetrics(dataframe, 'alpha_miner', alphaMinerResults['net'], alphaMinerResults['im'], alphaMinerResults['fm'])
    # Heuristic miner
    heuristicsMinerEvaluation = getMetrics(dataframe, 'heuristic_miner', heuristicsMinerResults['net'], heuristicsMinerResults['im'], heuristicsMinerResults['fm'])

    print({
        "inductiveMinerEvaluation": inductiveMinerEvaluation,
        "alphaMinerEvaluation": alphaMinerEvaluation,
        "heuristicsMinerEvaluation": heuristicsMinerEvaluation
    })