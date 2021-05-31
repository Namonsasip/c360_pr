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

from customer360.utilities.config_parser import l4_rolling_window


def usage_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            ###features_min
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_min",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min"],
                "int_l4_usage_postpaid_prepaid_weekly_features_min_2"
            ),
            node(
                l4_rolling_window,
                ["int_l4_usage_postpaid_prepaid_weekly_features_min_2",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_2"],
                "int_l4_usage_postpaid_prepaid_weekly_features_min_3"
            ),
            node(
                l4_rolling_window,
                ["int_l4_usage_postpaid_prepaid_weekly_features_min_3",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_3"],
                "int_l4_usage_postpaid_prepaid_weekly_features_min_4"
            ),
            node(
                l4_rolling_window,
                ["int_l4_usage_postpaid_prepaid_weekly_features_min_4",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_4"],
                "int_l4_usage_postpaid_prepaid_weekly_features_min_5"
            ),
            node(
                l4_rolling_window,
                ["int_l4_usage_postpaid_prepaid_weekly_features_min_5",
                 "params:l4_usage_postpaid_prepaid_weekly_features_min_5"],
                "l4_usage_postpaid_prepaid_weekly_features"
            ),

            # ###features_max
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_max",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_max_2"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_max_2",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_2"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_max_3"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_max_3",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_3"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_max_4"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_max_4",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_4"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_max_5"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_max_5",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_max_5"],
            #     "l4_usage_postpaid_prepaid_weekly_features_max"
            # ),
            #
            # ###features_avg
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_avg",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_2"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_2",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_2"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_3"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_3",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_3"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_4"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_4",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_4"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_5"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_5",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_5"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_6"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_6",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_6"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_7"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_7",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_7"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_8"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_8",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_8"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_avg_9"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_avg_9",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_avg_9"],
            #     "l4_usage_postpaid_prepaid_weekly_features_avg"
            # ),
            #
            # ###features_sum
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_postpaid_prepaid_weekly_for_l4_postpaid_prepaid_weekly_features_sum",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_sum_2"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_sum_2",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_2"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_sum_3"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_sum_3",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_3"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_sum_4"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_sum_4",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_4"],
            #     "int_l4_usage_postpaid_prepaid_weekly_features_sum_5"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["int_l4_usage_postpaid_prepaid_weekly_features_sum_5",
            #      "params:l4_usage_postpaid_prepaid_weekly_features_sum_5"],
            #     "l4_usage_postpaid_prepaid_weekly_features_sum"
            # ),

        ], name="usage_to_l4_pipeline"
    )
