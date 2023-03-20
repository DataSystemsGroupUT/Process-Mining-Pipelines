from pm4py.objects import log as log_lib
from pm4py.algo.evaluation.precision import utils as precision_utils
from pm4py.objects.petri_net.utils import align_utils as utils, check_soundness
from pm4py.statistics.start_activities.log.get import get_start_activities
from pm4py.objects.petri_net.utils.align_utils import get_visible_transitions_eventually_enabled_by_marking
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union, Tuple
from pm4py.objects.log.obj import EventLog, EventStream
from pm4py.objects.petri_net.obj import PetriNet, Marking
import pandas as pd
from pm4py.algo.evaluation.precision.variants.align_etconformance import align_fake_log_stop_marking, transform_markings_from_sync_to_original_net
from enum import Enum
from pm4py.util import constants

"""
    In order to use precision, there are 3 functions need to be executed in the following order:
    - apply (takes partition by partition)
    - aggregate (aggregates all results of each partition)
    - compute (computes the precision for the above aggregated result)
    
    Example:
        precision_alignments_0_40 = precision_alignment.apply(dataframe, net, im, fm) #dataframe[:40]
        precision_alignments_40_80 = precision_alignment.apply(dataframe, net, im, fm) #dataframe[40:80]
        precisions = precision_alignment.aggregate(precision_alignments_0_40, precision_alignments_40_80)
        result = precision_alignment.compute(**precisions, net=net, im=im, fm=fm)
"""

class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY

def apply(log: Union[EventLog, EventStream, pd.DataFrame], net: PetriNet, marking: Marking,
                      final_marking: Marking, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> float:
    """
    Get Align-ET Conformance precision

    Parameters
    ----------
    log
        Trace log
    net
        Petri net
    marking
        Initial marking
    final_marking
        Final marking
    parameters
        Parameters of the algorithm, including:
            Parameters.ACTIVITY_KEY -> Activity key
    """

    if parameters is None:
        parameters = {}


    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, log_lib.util.xes.DEFAULT_NAME_KEY)

    if not check_soundness.check_easy_soundness_net_in_fin_marking(net, marking, final_marking):
        raise Exception("trying to apply Align-ETConformance on a Petri net that is not a easy sound net!!")

    prefixes, prefix_count = precision_utils.get_log_prefixes(log, activity_key=activity_key)

    start_activities = set(get_start_activities(log, parameters=parameters))
    log_length = len(log)

    prefixes_keys = list(prefixes.keys())

    fake_log = precision_utils.form_fake_log(prefixes_keys)
    align_stop_marking = align_fake_log_stop_marking(fake_log, net, marking, final_marking)
    all_markings = transform_markings_from_sync_to_original_net(align_stop_marking, net)

    return {
        "prefixes": prefixes,
        "prefix_count": prefix_count,
        "start_activities": start_activities,
        "log_length": log_length,
        "all_markings": all_markings
    }

def compute(prefixes, prefix_count, log_length, start_activities, all_markings, net, im, fm, parameters={}):
    precision = 1.0
    sum_ee = 0
    sum_at = 0
    unfit = 0

    prefixes_keys = list(prefixes.keys())

    for i in range(len(prefixes)):
        markings = all_markings[i]

        if markings is not None:
            log_transitions = set(prefixes[prefixes_keys[i]])
            activated_transitions_labels = set()
            for m in markings:
                # add to the set of activated transitions in the model the activated transitions
                # for each prefix
                activated_transitions_labels = activated_transitions_labels.union(
                    x.label for x in utils.get_visible_transitions_eventually_enabled_by_marking(net, m) if
                    x.label is not None)
            escaping_edges = activated_transitions_labels.difference(log_transitions)

            sum_at += len(activated_transitions_labels) * prefix_count[prefixes_keys[i]]
            sum_ee += len(escaping_edges) * prefix_count[prefixes_keys[i]]

        else:
            unfit += prefix_count[prefixes_keys[i]]

    # fix: also the empty prefix should be counted!
    trans_en_ini_marking = set([x.label for x in get_visible_transitions_eventually_enabled_by_marking(net, im)])
    diff = trans_en_ini_marking.difference(start_activities)
    sum_at += log_length * len(trans_en_ini_marking)
    sum_ee += log_length * len(diff)
    # end fix

    if sum_at > 0:
        precision = 1 - float(sum_ee) / float(sum_at)

    return precision


def unionPrefixes(prefixes):
    result = {}
    for prefix in prefixes:
        for key, value in prefix.items():
            if key in result:
                result[key] = set().union(result[key], value)
            else:
                result[key] = value
    return result

# needs some work
def aggregate(firstPrecisionResult, secondPrecisionResult):
    all_prefixes = {}
    all_prefixes = unionPrefixes([firstPrecisionResult['prefixes'], secondPrecisionResult['prefixes']])
    all_prefix_count = firstPrecisionResult['prefix_count'] + secondPrecisionResult['prefix_count']
    all_start_activities = firstPrecisionResult['start_activities'].union(secondPrecisionResult['start_activities'])
    all_markings = firstPrecisionResult['all_markings'] + secondPrecisionResult['all_markings']
    log_length = firstPrecisionResult['log_length'] + secondPrecisionResult['log_length']

    return {
        "prefixes": all_prefixes,
        "prefix_count": all_prefix_count,
        "log_length": log_length,
        "start_activities": all_start_activities,
        "all_markings": all_markings
    }