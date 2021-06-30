# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

PLEASE DELETE THIS FILE ONCE YOU START WORKING ON YOUR OWN PROJECT!
"""

from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l4.to_l4 import l4_usage_rolling_window_weekly, \
    l4_usage_filter_date_rolling_window_weekly, l4_usage_filter_date_rolling_window_weekly_min, \
    l4_usage_rolling_window_weekly_min, build_l4_usage_rolling_window_split_column, merge_all_usage_massive_processing, \
    l4_usage_rolling_window_split_column_massive, l4_usage_rolling_window_split_column_maxdate
from customer360.utilities.config_parser import l4_rolling_window


def usage_to_l4_pipeline_max(**kwargs):
    return Pipeline(
        [
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set1"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set1"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set2"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set2"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set3"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set3"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set4"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set4"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set5"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set5"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set6"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set6"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set7"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set7"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set8"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set8"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set9"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set9"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set10"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set10"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set11"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set11"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set12"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set12"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_set13"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max_set13"
            # ),
            # node(
            #     merge_all_usage_massive_processing,
            #     ["l4_usage_postpaid_prepaid_weekly_features_max_set1",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set2",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set3",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set4",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set5",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set6",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set7",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set8",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set9",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set10",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set11",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set12",
            #      "l4_usage_postpaid_prepaid_weekly_features_max_set13"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max"
            # ),
            node(
                l4_usage_rolling_window_split_column_maxdate,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set1",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set2",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set3",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set4",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set5",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set6",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set7",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set8",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set9",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set10",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set11",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set12",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_set13",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max_tg"],
                "l4_usage_postpaid_prepaid_weekly_features_max"
            ),

        ]
    )


def usage_to_l4_pipeline_min(**kwargs):
    return Pipeline(
        [
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set1"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set1"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set2"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set2"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set3"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set3"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set4"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set4"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set5"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set5"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set6"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set6"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set7"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set7"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set8"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set8"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set9"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set9"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set10"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set10"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set11"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set11"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly_min,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_min_set12"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min_set12"
            # ),
            # node(
            #     l4_usage_rolling_window_weekly_min,
            #     ["l4_usage_postpaid_prepaid_weekly_features_min_set1",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set2",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set3",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set4",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set5",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set6",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set7",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set8",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set9",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set10",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set11",
            #      "l4_usage_postpaid_prepaid_weekly_features_min_set12"],
            #     "l4_usage_postpaid_prepaid_weekly_features_min"
            # ),
            node(
                l4_usage_rolling_window_split_column_massive,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set1",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set2",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set3",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set4",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set5",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set6",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set7",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set8",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set9",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set10",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set11",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set12",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_set13",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_tg"],
                "l4_usage_postpaid_prepaid_weekly_features_min"
            ),

        ]
    )


def usage_to_l4_pipeline_sum(**kwargs):
    return Pipeline(
        [
            # node(
            #     build_l4_usage_rolling_window_split_column,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_first",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_second",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_third",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_fourth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_fifth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_sixth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_seventh",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_eighth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_ninth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_tenth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_eleventh",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_twelfth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_thirteenth",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_tg"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum"
            # ),

            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_first"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_first"
            ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_second"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_second"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_third"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_third"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_fourth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_fourth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_fifth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_fifth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_sixth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_sixth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_seventh"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_seventh"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_eighth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_eighth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_ninth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_ninth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_tenth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_tenth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_eleventh"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_eleventh"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_twelfth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_twelfth"
            # ),
            # node(
            #     l4_usage_filter_date_rolling_window_weekly,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_thirteenth"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum_thirteenth"
            # ),
        ]
    )


def usage_to_l4_pipeline_sum1(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_second"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_second"
            ),
        ]
    )


def usage_to_l4_pipeline_sum2(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_third"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_third"
            ),

        ]
    )


def usage_to_l4_pipeline_sum3(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_fourth"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_fourth"
            ),

        ]
    )


def usage_to_l4_pipeline_sum4(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_fifth"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_fifth"
            ),

        ]
    )


def usage_to_l4_pipeline_sum5(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_sixth"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_sixth"
            ),

        ]
    )


def usage_to_l4_pipeline_sum6(**kwargs):
    return Pipeline(
        [
            node(
                l4_usage_filter_date_rolling_window_weekly,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum_seventh"],
                "l4_usage_postpaid_prepaid_weekly_features_sum_seventh"
            ),

        ]
    )


def usage_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
                 "params:l4_usage_postpaid_prepaid_weekly_features_max"],
                "l4_usage_postpaid_prepaid_weekly_features_max"

            ),
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min"],
                "l4_usage_postpaid_prepaid_weekly_features_min"
            ),
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_avg",
                 "params:l4_usage_postpaid_prepaid_weekly_features_avg"],
                "l4_usage_postpaid_prepaid_weekly_features_avg"
            ),
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
                 "params:l4_usage_postpaid_prepaid_weekly_features_sum"],
                "l4_usage_postpaid_prepaid_weekly_features_sum"
            ),

        ], name="usage_to_l4_pipeline"
    )
