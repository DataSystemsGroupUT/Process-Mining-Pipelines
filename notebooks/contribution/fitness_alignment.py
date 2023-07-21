from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.algo.conformance.alignments.decomposed import algorithm as decomp_alignments
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union
from pm4py.objects.log.obj import EventLog
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.util import typing
from enum import Enum
from pm4py.util import constants
from pm4py.objects.petri_net.utils.check_soundness import check_easy_soundness_net_in_fin_marking
from pm4py.algo.conformance.tokenreplay import algorithm as executor
from pm4py.algo.conformance.tokenreplay.variants import token_replay
from pm4py.util.xes_constants import DEFAULT_NAME_KEY

from pm4py import precision_alignments

class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    ATTRIBUTE_KEY = constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY
    CASE_ID_KEY = constants.PARAMETER_CONSTANT_CASEID_KEY
    TOKEN_REPLAY_VARIANT = "token_replay_variant"
    CLEANING_TOKEN_FLOOD = "cleaning_token_flood"
    MULTIPROCESSING = "multiprocessing"
    SHOW_PROGRESS_BAR = "show_progress_bar"

"""
    In order to use fitness, there are 3 functions need to be executed in the following order:
    - apply (takes partition by partition)
    - aggregate (aggregates all results of each partition)
    - compute (computes the precision for the above aggregated result)

    Example:
        fitness_0_40 = fitness_alignment.apply(dataframe, net, im, fm) #dataframe[:40]
        fitness_40_80 = fitness_alignment.apply(dataframe, net, im, fm) #dataframe[40:80]
        fitness_score = fitness_alignment.aggregate(fitness_0_40, fitness_40_80)
        result = fitness_alignment.compute(fitness_score)
"""

def apply_token_based(log: EventLog, petri_net: PetriNet, initial_marking: Marking, final_marking: Marking, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> Dict[str, float]:
    """
    Apply token replay fitness evaluation

    Parameters
    -----------
    log
        Trace log
    petri_net
        Petri net
    initial_marking
        Initial marking
    final_marking
        Final marking
    parameters
        Parameters

    Returns
    -----------
    dictionary
        Containing two keys (percFitTraces and averageFitness)
    """

    if parameters is None:
        parameters = {}
    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, DEFAULT_NAME_KEY)
    token_replay_variant = exec_utils.get_param_value(Parameters.TOKEN_REPLAY_VARIANT, parameters,
                                                      executor.Variants.TOKEN_REPLAY)
    cleaning_token_flood = exec_utils.get_param_value(Parameters.CLEANING_TOKEN_FLOOD, parameters, False)
    show_progress_bar = exec_utils.get_param_value(Parameters.SHOW_PROGRESS_BAR, parameters, True)
    case_id_key = exec_utils.get_param_value(Parameters.CASE_ID_KEY, parameters, constants.CASE_CONCEPT_NAME)

    parameters_tr = {token_replay.Parameters.ACTIVITY_KEY: activity_key,
                     token_replay.Parameters.CONSIDER_REMAINING_IN_FITNESS: True,
                     token_replay.Parameters.CLEANING_TOKEN_FLOOD: cleaning_token_flood,
                     token_replay.Parameters.SHOW_PROGRESS_BAR: show_progress_bar,
                     token_replay.Parameters.CASE_ID_KEY: case_id_key}

    aligned_traces = executor.apply(log, petri_net, initial_marking, final_marking, variant=token_replay_variant,
                                    parameters=parameters_tr)

    return aligned_traces

def compute_token_based(aligned_traces: typing.ListAlignments, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> Dict[str, float]:
    """
    Gets a dictionary expressing fitness in a synthetic way from the list of boolean values
    saying if a trace in the log is fit, and the float values of fitness associated to each trace

    Parameters
    ------------
    aligned_traces
        Result of the token-based replayer
    parameters
        Possible parameters of the evaluation

    Returns
    -----------
    dictionary
        Containing two keys (percFitTraces and averageFitness)
    """
    if parameters is None:
        parameters = {}
    no_traces = len(aligned_traces)
    fit_traces = len([x for x in aligned_traces if x["trace_is_fit"]])
    sum_of_fitness = sum([x["trace_fitness"] for x in aligned_traces])
    perc_fit_traces = 0.0
    average_fitness = 0.0
    log_fitness = 0
    total_m = sum([x["missing_tokens"] for x in aligned_traces])
    total_c = sum([x["consumed_tokens"] for x in aligned_traces])
    total_r = sum([x["remaining_tokens"] for x in aligned_traces])
    total_p = sum([x["produced_tokens"] for x in aligned_traces])
    if no_traces > 0 and total_c > 0 and total_p > 0:
        perc_fit_traces = float(100.0 * fit_traces) / float(no_traces)
        average_fitness = float(sum_of_fitness) / float(no_traces)
        log_fitness = 0.5 * (1 - total_m / total_c) + 0.5 * (1 - total_r / total_p)
    return {"perc_fit_traces": perc_fit_traces, "average_trace_fitness": average_fitness, "log_fitness": log_fitness,
            "percentage_of_fitting_traces": perc_fit_traces }


def apply(log: EventLog, petri_net: PetriNet, initial_marking: Marking, final_marking: Marking, align_variant=alignments.DEFAULT_VARIANT, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> Dict[str, float]:
    """
    Evaluate fitness based on alignments

    Parameters
    ----------------
    log
        Event log
    petri_net
        Petri net
    initial_marking
        Initial marking
    final_marking
        Final marking
    align_variant
        Variants of the alignments to apply
    parameters
        Parameters of the algorithm

    Returns
    ---------------
    dictionary
        Containing two keys (percFitTraces and averageFitness)
    """
    if not (check_easy_soundness_net_in_fin_marking(petri_net, initial_marking,final_marking)):
        alignment_result = apply_token_based(log, petri_net, initial_marking, final_marking)
    else:
        alignment_result = alignments.apply(log, petri_net, initial_marking, final_marking, variant=align_variant, parameters=parameters)
    return alignment_result

def compute(aligned_traces: typing.ListAlignments, net, im, fm) -> Dict[str, float]:
    if not (check_easy_soundness_net_in_fin_marking(net, im, fm)):
        return compute_token_based(aligned_traces)
    """
    Transforms the alignment result to a simple dictionary
    including the percentage of fit traces and the average fitness

    Parameters
    ----------
    aligned_traces
        Alignments calculated for the traces in the log
    parameters
        Possible parameters of the evaluation

    Returns
    ----------
    dictionary
        Containing two keys (percFitTraces and averageFitness)
    """
    no_traces = len([x for x in aligned_traces if x is not None])
    no_fit_traces = 0
    sum_fitness = 0.0
    sum_bwc = 0.0
    sum_cost = 0.0

    for tr in aligned_traces:
        if tr is not None:
            if tr["fitness"] == 1.0:
                no_fit_traces = no_fit_traces + 1
            sum_fitness += tr["fitness"]
            sum_bwc += tr["bwc"]
            sum_cost += tr["cost"]

    perc_fit_traces = 0.0
    average_fitness = 0.0
    log_fitness = 0.0

    if no_traces > 0:
        perc_fit_traces = (100.0 * float(no_fit_traces)) / (float(no_traces))
        average_fitness = float(sum_fitness) / float(no_traces)
        log_fitness = 1.0 - float(sum_cost) / float(sum_bwc)

    return {"percFitTraces": perc_fit_traces, "averageFitness": average_fitness,
            "percentage_of_fitting_traces": perc_fit_traces,
            "average_trace_fitness": average_fitness, "log_fitness": log_fitness}


def aggregate(firstFitnessResult, secondFitnessResult):
    return firstFitnessResult + secondFitnessResult