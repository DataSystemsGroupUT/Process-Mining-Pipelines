from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.algo.conformance.alignments.decomposed import algorithm as decomp_alignments
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union
from pm4py.objects.log.obj import EventLog
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.util import typing
from enum import Enum
from pm4py.util import constants

class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY


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

    alignment_result = alignments.apply(log, petri_net, initial_marking, final_marking, variant=align_variant,
                                                parameters=parameters)
    return alignment_result

def compute(aligned_traces: typing.ListAlignments, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> Dict[str, float]:
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
    if parameters is None:
        parameters = {}
    str(parameters)
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