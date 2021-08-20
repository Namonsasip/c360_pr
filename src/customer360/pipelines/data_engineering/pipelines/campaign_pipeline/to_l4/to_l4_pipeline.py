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

from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_ranked_window
from customer360.pipelines.data_engineering.nodes.campaign_nodes.to_l4 import add_relative_time_features, \
    build_campaign_weekly_features, add_column_run_date


def campaign_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_campaign_weekly_features,
                ["l2_campaign_postpaid_prepaid_weekly",
                 "params:l4_campaign_postpaid_prepaid_features_first_first",
                 "params:l4_campaign_postpaid_prepaid_features_first_second",
                 "params:l4_campaign_postpaid_prepaid_features_second_first",
                 "params:l4_campaign_postpaid_prepaid_features_second_second",
                 "params:l4_campaign_postpaid_prepaid_features_second_second_second",
                 "params:l4_campaign_postpaid_prepaid_features_third_first",
                 # "params:l4_campaign_postpaid_prepaid_features_third_first_second",
                 "params:l4_campaign_postpaid_prepaid_features_third_second",
                 # "params:l4_campaign_postpaid_prepaid_features_third_second_second",
                 "params:l4_campaign_postpaid_prepaid_features_fourth_first",
                 "params:l4_campaign_postpaid_prepaid_features_fourth_first_second",
                 "params:l4_campaign_postpaid_prepaid_features_fourth_second",
                 "params:l4_campaign_postpaid_prepaid_features_fourth_second_second",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_first",
                 # "params:l4_campaign_postpaid_prepaid_features_fifth_first_first",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_first_second",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_first_third",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_second",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_second_second",
                 "params:l4_campaign_postpaid_prepaid_features_fifth_second_third",
                 "params:l4_campaign_postpaid_prepaid_features_sixth_first",
                 "params:l4_campaign_postpaid_prepaid_features_sixth_first_second",
                 "params:l4_campaign_postpaid_prepaid_features_sixth_second",
                 "params:l4_campaign_postpaid_prepaid_features_sixth_second_second",
                 ],
                "l4_campaign_postpaid_prepaid_int"

            ),
            node(
                add_relative_time_features, ['l4_campaign_postpaid_prepaid_int'],
                'l4_campaign_postpaid_prepaid_features'
            ),

        ], name="campaign_to_l4_pipeline"
    )


def campaign_to_l4_ranking_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_campaign_top_channel_weekly",
                 "params:l4_campaign_top_channel_features_int"],
                "l4_campaign_top_channel_weekly_int"

            ),
            node(
                l4_rolling_ranked_window, ["l4_campaign_top_channel_weekly_int",
                                           "params:l4_campaign_top_channel_features"],
                "l4_campaign_top_channel_features_temp"

            ),
            node(
                add_column_run_date, ['l4_campaign_top_channel_features_temp'],
                "l4_campaign_top_channel_features"
            ),

        ], name="campaign_to_l4_ranking_pipeline"
    )
