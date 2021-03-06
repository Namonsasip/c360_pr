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

from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l3 import build_digital_l3_monthly_features
from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l3.to_l3_nodes import *


def digital_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_digital_l3_monthly_features,
                ["l0_digital_cxenxse_user_profile_monthly",
                 "l3_customer_profile_union_monthly_feature_for_l3_digital_cxenxse_user_profile_monthly",
                 "params:l3_digital_cxenxse_user_profile_monthly",
                 "params:exception_partition_list_for_l0_digital_cxenxse_user_profile_monthly"],
                "l3_digital_cxenxse_user_profile_monthly"
            ),
        ], name="digital_to_l3_pipeline"
    )

########################################## App agg category monthly  ###############################################
def digital_to_l3_app_monthly_feature_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_agg_monthly,
                inputs=[
                    "l1_digital_customer_app_category_agg_daily_catlv_1",
                    "params:l3_digital_app_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_app_category_agg_monthly_catlv_1",
                tags=["node_digital_app_monthly_feature_catlv_1"],
            ),
            node(
                func=digital_mobile_app_category_agg_monthly,
                inputs=[
                    "l1_digital_customer_app_category_agg_daily_catlv_2",
                    "params:l3_digital_app_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_app_category_agg_monthly_catlv_2",
                tags=["node_digital_app_monthly_feature_catlv_2"],
            ),
            node(
                func=digital_mobile_app_category_agg_monthly,
                inputs=[
                    "l1_digital_customer_app_category_agg_daily_catlv_3",
                    "params:l3_digital_app_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_app_category_agg_monthly_catlv_3",
                tags=["node_digital_app_monthly_feature_catlv_3"],
            ),
            node(
                func=digital_mobile_app_category_agg_monthly,
                inputs=[
                    "l1_digital_customer_app_category_agg_daily_catlv_4",
                    "params:l3_digital_app_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_app_category_agg_monthly_catlv_4",
                tags=["node_digital_app_monthly_feature_catlv_4"],
            ),
             node(
                func=digital_mobile_app_agg_monthly,
                inputs=[
                    "l0_digital_mobile_app_daily_for_mobile_app_monthly",
                    "params:l3_digital_app_agg_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_app_agg_monthly",
                tags=["node_digital_app_agg_monthly_feature"],
            ),
        ], name="digital_app_monthly_feature_pipeline"
    )

########################################## App category favorite monthly  ###############################################
def digital_to_l3_app_monthly_feature_favorite(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_app_category_agg_monthly_catlv_1",
                    "params:l3_digital_mobile_app_category_favorite_total_monthly",
                    "params:l3_digital_mobile_app_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_app_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_app_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_catlv_1",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_1"],
            ),
            node(
                func=digital_mobile_app_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_app_category_agg_monthly_catlv_2",
                    "params:l3_digital_mobile_app_category_favorite_total_monthly",
                    "params:l3_digital_mobile_app_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_app_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_app_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_catlv_2",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_2"],
            ),
            node(
                func=digital_mobile_app_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_app_category_agg_monthly_catlv_3",
                    "params:l3_digital_mobile_app_category_favorite_total_monthly",
                    "params:l3_digital_mobile_app_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_app_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_app_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_catlv_3",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_3"],
            ),
            node(
                func=digital_mobile_app_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_app_category_agg_monthly_catlv_4",
                    "params:l3_digital_mobile_app_category_favorite_total_monthly",
                    "params:l3_digital_mobile_app_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_app_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_app_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_catlv_4",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_4"],
            ),
            ################# mobile app favorite by application monthly ##############
            node(
                func=digital_mobile_app_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "params:l3_digital_customer_app_favorite_by_category_sql",
                    "params:l3_digital_customer_app_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_app_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_app_favorite_by_category_sql_volume",
                    "params:category_level_1"
                ],
                outputs="l3_digital_customer_app_category_favorite_by_category_monthly_catlv_1",
                tags=["l3_digital_mobile_app_favorite_by_category_monthly_catlv_1"],
            ),
            node(
                func=digital_mobile_app_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "params:l3_digital_customer_app_favorite_by_category_sql",
                    "params:l3_digital_customer_app_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_app_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_app_favorite_by_category_sql_volume",
                    "params:category_level_2"
                ],
                outputs="l3_digital_customer_app_category_favorite_by_category_monthly_catlv_2",
                tags=["l3_digital_mobile_app_favorite_by_category_monthly_catlv_2"],
            ),
            node(
                func=digital_mobile_app_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "params:l3_digital_customer_app_favorite_by_category_sql",
                    "params:l3_digital_customer_app_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_app_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_app_favorite_by_category_sql_volume",
                    "params:category_level_3"
                ],
                outputs="l3_digital_customer_app_category_favorite_by_category_monthly_catlv_3",
                tags=["l3_digital_mobile_app_favorite_by_category_monthly_catlv_3"],
            ),
            node(
                func=digital_mobile_app_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "params:l3_digital_customer_app_favorite_by_category_sql",
                    "params:l3_digital_customer_app_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_app_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_app_favorite_by_category_sql_volume",
                    "params:category_level_4"
                ],
                outputs="l3_digital_customer_app_category_favorite_by_category_monthly_catlv_4",
                tags=["l3_digital_mobile_app_favorite_by_category_monthly_catlv_4"],
            ),
        ], name="digital_app_monthly_feature_pipeline"
    )

########################################## App category score monthly  ###############################################
def digital_to_l3_app_monthly_feature_score(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_app_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_1",
                    "params:l3_digital_customer_app_score_sql",
                    "params:l3_digital_customer_app_score_sql_sum"
                ],
                outputs="l3_digital_customer_app_category_score_monthly_catlv_1",
                tags=["digital_customer_app_category_score_monthly_catlv_1"],
            ),
            node(
                func=l3_digital_mobile_app_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_2",
                    "params:l3_digital_customer_app_score_sql",
                    "params:l3_digital_customer_app_score_sql_sum"
                ],
                outputs="l3_digital_customer_app_category_score_monthly_catlv_2",
                tags=["digital_customer_app_category_score_monthly_catlv_2"],
            ),
            node(
                func=l3_digital_mobile_app_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_3",
                    "params:l3_digital_customer_app_score_sql",
                    "params:l3_digital_customer_app_score_sql_sum"
                ],
                outputs="l3_digital_customer_app_category_score_monthly_catlv_3",
                tags=["digital_customer_app_category_score_monthly_catlv_3"],
            ),
            node(
                func=l3_digital_mobile_app_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_4",
                    "params:l3_digital_customer_app_score_sql",
                    "params:l3_digital_customer_app_score_sql_sum"
                ],
                outputs="l3_digital_customer_app_category_score_monthly_catlv_4",
                tags=["digital_customer_app_category_score_monthly_catlv_4"],
            ),
        ], name="digital_app_monthly_feature_score_pipeline"
    )

########################################## App agg category timeband monthly  ###############################################
def digital_to_l3_app_agg_timeband_monthly_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_morning_catlv_1",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_morning_catlv_1",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_1",
                tags=["digital_customer_app_category_agg_timeband_monthly_morning_catlv_1"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_afternoon_catlv_1",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_afternoon_catlv_1",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_1",
                tags=["digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_1"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_evening_catlv_1",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_evening_catlv_1",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_1",
                tags=["digital_customer_app_category_agg_timeband_monthly_evening_catlv_1"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_night_catlv_1",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_night_catlv_1",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_1",
                tags=["digital_customer_app_category_agg_timeband_monthly_night_catlv_1"]
            ), #App agg timeband monthly level 1
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_morning_catlv_2",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_morning_catlv_2",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_2",
                tags=["digital_customer_app_category_agg_timeband_monthly_morning_catlv_2"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_afternoon_catlv_2",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_afternoon_catlv_2",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_2",
                tags=["digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_2"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_evening_catlv_2",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_evening_catlv_2",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_2",
                tags=["digital_customer_app_category_agg_timeband_monthly_evening_catlv_2"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_night_catlv_2",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_night_catlv_2",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_2",
                tags=["digital_customer_app_category_agg_timeband_monthly_night_catlv_2"]
            ), #App agg timeband monthly level 2
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_morning_catlv_3",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_morning_catlv_3",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_3",
                tags=["digital_customer_app_category_agg_timeband_monthly_morning_catlv_3"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_afternoon_catlv_3",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_afternoon_catlv_3",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_3",
                tags=["digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_3"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_evening_catlv_3",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_evening_catlv_3",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_3",
                tags=["digital_customer_app_category_agg_timeband_monthly_evening_catlv_3"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_night_catlv_3",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_night_catlv_3",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_3",
                tags=["digital_customer_app_category_agg_timeband_monthly_night_catlv_3"]
            ), #App agg timeband monthly level 3
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_morning_catlv_4",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_morning_catlv_4",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_4",
                tags=["digital_customer_app_category_agg_timeband_monthly_morning_catlv_4"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_afternoon_catlv_4",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_afternoon_catlv_4",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_4",
                tags=["digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_4"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_evening_catlv_4",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_evening_catlv_4",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_4",
                tags=["digital_customer_app_category_agg_timeband_monthly_evening_catlv_4"]
            ),
            node(
                func=digital_customer_app_category_agg_timeband_monthly,
                inputs=["l1_digital_customer_app_category_agg_timeband_for_l3_night_catlv_4",
                        "l3_digital_customer_app_category_agg_for_l3_monthly_night_catlv_4",
                        "params:l3_digital_customer_app_agg_category_timeband_sql"],
                outputs="l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_4",
                tags=["digital_customer_app_category_agg_timeband_monthly_night_catlv_4"]
            ), #App agg timeband monthly level 4
        ]
    )

########################################## App agg category favorite timeband monthly  ###############################################
def digital_to_l3_app_favorite_timeband_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                    func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                    inputs=[
                        "l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_1",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                        ],
                    outputs="l3_digital_customer_app_category_favorite_monthly_timeband_morning_catlv_1",
                    tags="l3_digital_customer_app_category_favorite_monthly_morning_catlv_1"
            ),
            node(
                    func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                    inputs=[
                        "l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_1",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                        ],
                    outputs="l3_digital_customer_app_category_favorite_monthly_timeband_afternoon_catlv_1",
                    tags="l3_digital_customer_app_category_favorite_monthly_afternoon_catlv_1"
            ),
            node(
                    func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                    inputs=[
                        "l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_1",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                        ],
                    outputs="l3_digital_customer_app_category_favorite_monthly_timeband_evening_catlv_1",
                    tags="l3_digital_customer_app_category_favorite_monthly_evening_catlv_1"
            ),
            node(
                    func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                    inputs=[
                        "l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_1",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                        "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                        ],
                    outputs="l3_digital_customer_app_category_favorite_monthly_timeband_night_catlv_1",
                    tags="l3_digital_customer_app_category_favorite_monthly_night_catlv_1"
            ), #App agg favourite timeband monthly level 1
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_2",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_morning_catlv_2",
                tags="l3_digital_customer_app_category_favorite_monthly_morning_catlv_2"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_2",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_afternoon_catlv_2",
                tags="l3_digital_customer_app_category_favorite_monthly_afternoon_catlv_2"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_2",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_evening_catlv_2",
                tags="l3_digital_customer_app_category_favorite_monthly_evening_catlv_2"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_2",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_night_catlv_2",
                tags="l3_digital_customer_app_category_favorite_monthly_night_catlv_2"
            ), #App agg favourite timeband monthly level 2
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_3",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_morning_catlv_3",
                tags="l3_digital_customer_app_category_favorite_monthly_morning_catlv_3"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_3",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_afternoon_catlv_3",
                tags="l3_digital_customer_app_category_favorite_monthly_afternoon_catlv_3"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_3",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_evening_catlv_3",
                tags="l3_digital_customer_app_category_favorite_monthly_evening_catlv_3"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_3",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_night_catlv_3",
                tags="l3_digital_customer_app_category_favorite_monthly_night_catlv_3"
            ), #App agg favourite timeband monthly level 3
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_4",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_morning_catlv_4",
                tags="l3_digital_customer_app_category_favorite_monthly_morning_catlv_4"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_4",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_afternoon_catlv_4",
                tags="l3_digital_customer_app_category_favorite_monthly_afternoon_catlv_4"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_4",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_evening_catlv_4",
                tags="l3_digital_customer_app_category_favorite_monthly_evening_catlv_4"
            ),
            node(
                func=l3_digital_mobile_app_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_4",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_app_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_app_category_favorite_monthly_timeband_night_catlv_4",
                tags="l3_digital_customer_app_category_favorite_monthly_night_catlv_4"
            ), #App agg favourite timeband monthly level 4
        ], name="digital_to_l3_digital_mobile_web_agg_monthly"
    )
########### Web agg monthly by cat and domain ########
def digital_to_l3_digital_mobile_web_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_web_category_agg_monthly,
                inputs="l1_digital_customer_web_category_agg_daily_catlv_1",
                outputs="l3_digital_customer_web_category_agg_monthly_catlv_1",
                tags="l3_digital_mobile_web_category_agg_monthly_catlv_1"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_monthly,
                inputs="l1_digital_customer_web_category_agg_daily_catlv_2",
                outputs="l3_digital_customer_web_category_agg_monthly_catlv_2",
                tags="l3_digital_mobile_web_category_agg_monthly_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_monthly,
                inputs="l1_digital_customer_web_category_agg_daily_catlv_3",
                outputs="l3_digital_customer_web_category_agg_monthly_catlv_3",
                tags="l3_digital_mobile_web_category_agg_monthly_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_monthly,
                inputs="l1_digital_customer_web_category_agg_daily_catlv_4",
                outputs="l3_digital_customer_web_category_agg_monthly_catlv_4",
                tags="l3_digital_mobile_web_category_agg_monthly_catlv_4"
            ),
            # node(
            #     func=digital_mobile_web_agg_sum_monthly,
            #     inputs=
            #     [
            #         "l0_digital_mobile_web_daily_for_mobile_web_monthly",
            #         "params:l3_digital_web_agg_monthly_feature_pipeline",
            #     ],
            #     outputs="int_l3_digital_customer_web_agg_monthly",
            #     tags=["node_digital_web_monthly_feature"],
            # ),
            #  node(
            #     func=digital_mobile_web_agg_monthly,
            #     inputs=
            #     [
            #         "int_l3_digital_customer_web_agg_monthly",
            #         "l1_digital_aib_categories_clean_for_l3_digital_mobile_web_agg_monthly",
            #     ],
            #     outputs="l3_digital_customer_web_agg_monthly",
            #     tags=["node_digital_web_monthly_feature"],
            # ),
        ], name="digital_to_l3_digital_mobile_web_agg_monthly"
    )

############### Web agg monthly score ###################
def digital_to_l3_web_monthly_feature_score(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_web_category_score_monthly,
                inputs=[
                    "l3_digital_customer_web_category_favorite_monthly_catlv_1",
                    "params:l3_digital_customer_web_score_sql",
                    "params:l3_digital_customer_web_score_sql_sum",
                ],
                outputs="l3_digital_customer_web_category_score_monthly_catlv_1",
                tags=["digital_customer_web_category_score_monthly_catlv_1"],
            ),
            node(
                func=l3_digital_mobile_web_category_score_monthly,
                inputs=[
                    "l3_digital_customer_web_category_favorite_monthly_catlv_2",
                    "params:l3_digital_customer_web_score_sql",
                    "params:l3_digital_customer_web_score_sql_sum",
                ],
                outputs="l3_digital_customer_web_category_score_monthly_catlv_2",
                tags=["digital_customer_web_category_score_monthly_catlv_2"],
            ),
            node(
                func=l3_digital_mobile_web_category_score_monthly,
                inputs=[
                    "l3_digital_customer_web_category_favorite_monthly_catlv_3",
                    "params:l3_digital_customer_web_score_sql",
                    "params:l3_digital_customer_web_score_sql_sum",
                ],
                outputs="l3_digital_customer_web_category_score_monthly_catlv_3",
                tags=["digital_customer_web_category_score_monthly_catlv_3"],
            ),
            node(
                func=l3_digital_mobile_web_category_score_monthly,
                inputs=[
                    "l3_digital_customer_web_category_favorite_monthly_catlv_4",
                    "params:l3_digital_customer_web_score_sql",
                    "params:l3_digital_customer_web_score_sql_sum",
                ],
                outputs="l3_digital_customer_web_category_score_monthly_catlv_4",
                tags=["digital_customer_web_category_score_monthly_catlv_4"],
            ),
        ], name="digital_web_monthly_feature_score_pipeline"
    )

############### Web agg timeband by category ###################
def digital_to_l3_digital_mobile_web_agg_timeband(**kwargs):
    return Pipeline(
        [
            ############## Category_level_1 ###################
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_morning_catlv_1",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_1",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_morning_catlv_1",
                tags="l3_digital_customer_web_category_agg_timeband"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_1",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                tags="l3_digital_customer_web_category_agg_timeband"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_evening_catlv_1",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_1",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_evening_catlv_1",
                tags="l3_digital_customer_web_category_agg_timeband"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_night_catlv_1",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_1",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_night_catlv_1",
                tags="l3_digital_customer_web_category_agg_timeband"
            ), #Web agg timeband monthly level 1
            ############## Category_level_2 ###################
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_morning_catlv_2",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_2",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_morning_catlv_2",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_2",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_evening_catlv_2",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_2",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_evening_catlv_2",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_night_catlv_2",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_2",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_night_catlv_2",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_2"
            ), #Web agg timeband monthly level 2
            ############## Category_level_3 ###################
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_morning_catlv_3",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_3",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_morning_catlv_3",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_3",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_evening_catlv_3",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_3",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_evening_catlv_3",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_night_catlv_3",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_3",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_night_catlv_3",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_3"
            ), #Web agg timeband monthly level 3
            ############## Category_level_4 ###################
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_morning_catlv_4",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_4",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_morning_catlv_4",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_4",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_evening_catlv_4",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_4",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_evening_catlv_4",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_agg_timeband,
                inputs=["l1_digital_customer_web_category_agg_timeband_night_catlv_4",
                        "l3_digital_customer_web_category_agg_monthly_for_l3_digital_mobile_web_category_agg_timeband_catlv_4",
                        "params:l3_digital_customer_web_agg_category_timeband_sql"],
                outputs="l3_digital_customer_web_category_agg_timeband_night_catlv_4",
                tags="l3_digital_customer_web_category_agg_timeband_catlv_4"
            ), #Web agg timeband monthly level 1
        ],
    )

############### Web agg monthly favourite  ###################
def digital_to_l3_web_monthly_feature_favorite(**kwargs):
    return Pipeline(
        [
            ############## Fav by cat ###################
            node(
                func=digital_mobile_web_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_catlv_1",
                    "params:l3_digital_mobile_web_category_favorite_total_monthly",
                    "params:l3_digital_mobile_web_category_favorite_transaction_monthly",
                    "params:l3_digital_mobile_web_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_web_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_web_category_favorite_monthly_catlv_1",
                tags=["digital_mobile_web_category_favorite_monthly_catlv_1"],
            ),
            node(
                func=digital_mobile_web_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_catlv_2",
                    "params:l3_digital_mobile_web_category_favorite_total_monthly",
                    "params:l3_digital_mobile_web_category_favorite_transaction_monthly",
                    "params:l3_digital_mobile_web_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_web_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_web_category_favorite_monthly_catlv_2",
                tags=["digital_mobile_web_category_favorite_monthly_catlv_2"],
            ),
            node(
                func=digital_mobile_web_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_catlv_3",
                    "params:l3_digital_mobile_web_category_favorite_total_monthly",
                    "params:l3_digital_mobile_web_category_favorite_transaction_monthly",
                    "params:l3_digital_mobile_web_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_web_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_web_category_favorite_monthly_catlv_3",
                tags=["digital_mobile_web_category_favorite_monthly_catlv_3"],
            ),
            node(
                func=digital_mobile_web_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_catlv_4",
                    "params:l3_digital_mobile_web_category_favorite_total_monthly",
                    "params:l3_digital_mobile_web_category_favorite_transaction_monthly",
                    "params:l3_digital_mobile_web_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_web_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_web_category_favorite_monthly_catlv_4",
                tags=["digital_mobile_web_category_favorite_monthly_catlv_4"],
            ),
            ############# Fav by domain ###################
            node(
                func=digital_mobile_web_favorite_by_category_monthly,
                inputs=[
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_web_category_favorite_by_category_monthly_catlv_1",
                    "params:l3_digital_customer_web_favorite_by_category_sql",
                    "params:l3_digital_customer_web_favorite_by_category_sql_transaction",
                    "params:l3_digital_customer_web_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_web_favorite_by_category_sql_volume",
                    "params:category_level_1"
                ],
                outputs="l3_digital_customer_web_category_favorite_by_category_monthly_catlv_1",
                tags=["l3_digital_mobile_web_favorite_by_category_monthly"],
            ),
            node(
                func=digital_mobile_web_favorite_by_category_monthly,
                inputs=[
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_web_category_favorite_by_category_monthly_catlv_2",
                    "params:l3_digital_customer_web_favorite_by_category_sql",
                    "params:l3_digital_customer_web_favorite_by_category_sql_transaction",
                    "params:l3_digital_customer_web_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_web_favorite_by_category_sql_volume",
                    "params:category_level_2"
                ],
                outputs="l3_digital_customer_web_category_favorite_by_category_monthly_catlv_2",
                tags=["l3_digital_mobile_web_favorite_by_category_monthly_catlv_2"],
            ),
            node(
                func=digital_mobile_web_favorite_by_category_monthly,
                inputs=[
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_web_category_favorite_by_category_monthly_catlv_3",
                    "params:l3_digital_customer_web_favorite_by_category_sql",
                    "params:l3_digital_customer_web_favorite_by_category_sql_transaction",
                    "params:l3_digital_customer_web_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_web_favorite_by_category_sql_volume",
                    "params:category_level_3"
                ],
                outputs="l3_digital_customer_web_category_favorite_by_category_monthly_catlv_3",
                tags=["l3_digital_mobile_web_favorite_by_category_monthly_catlv_3"],
            ),
            node(
                func=digital_mobile_web_favorite_by_category_monthly,
                inputs=[
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_web_category_favorite_by_category_monthly_catlv_4",
                    "params:l3_digital_customer_web_favorite_by_category_sql",
                    "params:l3_digital_customer_web_favorite_by_category_sql_transaction",
                    "params:l3_digital_customer_web_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_web_favorite_by_category_sql_volume",
                    "params:category_level_4"
                ],
                outputs="l3_digital_customer_web_category_favorite_by_category_monthly_catlv_4",
                tags=["l3_digital_mobile_web_favorite_by_category_monthly_catlv_4"],
            ),
        ], name="digital_web_monthly_feature_pipeline"
    )

############### Web agg monthly favourite timeband ###################
def digital_to_l3_web_favorite_timeband_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_morning_catlv_1",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                    ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_1",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_1"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_1",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_1"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_evening_catlv_1",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_1",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_1"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_night_catlv_1",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_1",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_1"
            ), #Web agg monthly favourite timeband monthly level 1
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_morning_catlv_2",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                    ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_2",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_2",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_evening_catlv_2",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_2",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_2"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_night_catlv_2",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_2",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_2"
            ), #Web agg monthly favourite timeband monthly level 2
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_morning_catlv_3",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                    ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_3",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_3",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_evening_catlv_3",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_3",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_3"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_night_catlv_3",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_3",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_3"
            ), #Web agg monthly favourite timeband monthly level 3
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_morning_catlv_4",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                    ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_4",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_morning_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_4",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_afternoon_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_evening_catlv_4",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_4",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_evening_catlv_4"
            ),
            node(
                func=l3_digital_mobile_web_category_favorite_monthly_timeband,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_night_catlv_4",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_transection",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_duration",
                    "params:l3_digital_customer_web_category_favorite_timeband_sql_volume",
                ],
                outputs="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_4",
                tags="l3_digital_customer_web_category_favorite_timeband_monthly_night_catlv_4"
            ), #Web agg monthly favourite timeband monthly level 4
        ], name="digital_to_l3_digital_mobile_web_agg_monthly"
    )

########################################## Combine agg category  monthly  ###############################################
def digital_to_l3_digital_combine_feature_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_to_l3_digital_combine_agg_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_daily_for_l3_combine_category_monthly_catlv_1",
                    "params:l3_digital_combine_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_combine_category_agg_monthly_catlv_1",
                tags=["l3_digital_customer_combine_category_agg_monthly_catlv_1"],
            ),
            node(
                func=digital_to_l3_digital_combine_agg_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_daily_for_l3_combine_category_monthly_catlv_2",
                    "params:l3_digital_combine_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_combine_category_agg_monthly_catlv_2",
                tags=["l3_digital_customer_combine_category_agg_monthly_catlv_2"],
            ),
            node(
                func=digital_to_l3_digital_combine_agg_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_daily_for_l3_combine_category_monthly_catlv_3",
                    "params:l3_digital_combine_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_combine_category_agg_monthly_catlv_3",
                tags=["l3_digital_customer_combine_category_agg_monthly_catlv_3"],
            ),
            node(
                func=digital_to_l3_digital_combine_agg_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_daily_for_l3_combine_category_monthly_catlv_4",
                    "params:l3_digital_combine_monthly_feature_pipeline",
                ],
                outputs="l3_digital_customer_combine_category_agg_monthly_catlv_4",
                tags=["l3_digital_customer_combine_category_agg_monthly_catlv_4"],
            ),
        ], name="digital_to_l3_digital_mobile_combine_agg_monthly"
    )

########################################## Combine category favrite  monthly  ###############################################
def digital_to_l3_combine_monthly_feature_favorite(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_combine_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1",
                    "params:l3_digital_mobile_combine_category_favorite_total_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_combine_category_favorite_monthly_catlv_1",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_1"],
            ),
            node(
                func=digital_mobile_combine_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2",
                    "params:l3_digital_mobile_combine_category_favorite_total_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_combine_category_favorite_monthly_catlv_2",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_2"],
            ),
            node(
                func=digital_mobile_combine_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3",
                    "params:l3_digital_mobile_combine_category_favorite_total_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_combine_category_favorite_monthly_catlv_3",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_3"],
            ),
            node(
                func=digital_mobile_combine_category_favorite_monthly,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_4",
                    "params:l3_digital_mobile_combine_category_favorite_total_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_Transaction_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_duration_monthly",
                    "params:l3_digital_mobile_combine_category_favorite_volume_monthly"
                ],
                outputs="l3_digital_customer_combine_category_favorite_monthly_catlv_4",
                tags=["digital_mobile_app_category_favorite_monthly_catlv_4"],
            ),
        ], name="digital_app_monthly_feature_pipeline"
    )

def digital_to_l3_customer_relay_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_relay_conversion_agg_monthly,
                inputs=["l0_digital_relay_engagement_conversion_for_agg_monthly",
                        "l0_digital_relay_engagement_conversion_package_for_agg_monthly",
                        "params:l3_digital_relay_engagement_conversion_count_visit_by_cid_monthly",
                        "params:l3_digital_relay_engagement_conversion_package_count_visit_by_cid_monthly",
                        ],
                outputs="l3_digital_customer_relay_conversion_agg_monthly",
                tags=["digital_customer_relay_conversion_agg_monthly"],
            ),
            node(
                func=digital_customer_relay_pageview_fav_monthly,
                inputs=[
                    "l0_digital_relay_engagement_pageview_for_fav_monthly",
                    "l0_digital_relay_engagement_productinfo_for_fav_monthly",
                    "params:l3_digital_relay_engagement_pageview_count_visit_monthly",
                    "params:l3_digital_relay_popular_url_by_pageviews_monthly",
                    "params:l3_digital_relay_popular_subcategory1_by_pageviews_monthly",
                    "params:l3_digital_relay_popular_subcategory2_by_pageviews_monthly",
                    "params:l3_digital_relay_popular_cid_by_pageviews_monthly",
                    "params:l3_digital_relay_popular_productname_by_productinfo_monthly",
                    "params:l3_digital_relay_most_popular_url_by_pageviews_monthly",
                    "params:l3_digital_relay_most_popular_subcategory1_by_pageviews_monthly",
                    "params:l3_digital_relay_most_popular_subcategory2_by_pageviews_monthly",
                    "params:l3_digital_relay_most_popular_cid_by_pageviews_monthly",
                    "params:l3_digital_relay_most_popular_productname_by_productinfo_monthly"
                ],
                outputs="l3_digital_customer_relay_pageview_fav_monthly",
                tags=["digital_customer_relay_pageview_fav_monthly"],
            ),
                node(
                func=digital_customer_relay_conversion_fav_monthly,
                inputs=[
                    "l0_digital_relay_engagement_conversion_for_fav_monthly",
                    "params:l3_digital_relay_popular_product_by_engagement_conversion_monthly",
                    "params:l3_digital_relay_popular_cid_by_engagement_conversion_monthly",
                    "params:l3_digital_relay_most_popular_product_by_engagement_conversion_monthly",
                    "params:l3_digital_relay_most_popular_cid_by_engagement_conversion_monthly",
                ],
                outputs="l3_digital_customer_relay_conversion_fav_monthly",
                tags=["digital_customer_relay_conversion_fav_monthly"],
            ),
            node(
                func=digital_customer_relay_conversion_package_fav_monthly,
                inputs=[
                    "l0_digital_relay_engagement_conversion_package_for_fav_monthly",
                    "params:l3_digital_relay_popular_product_by_engagement_conversion_package_monthly",
                    "params:l3_digital_relay_popular_cid_by_engagement_conversion_package_monthly",
                    "params:l3_digital_relay_most_popular_product_by_engagement_conversion_package_monthly",
                    "params:l3_digital_relay_most_popular_cid_by_engagement_conversion_package_monthly",
                ],
                outputs="l3_digital_customer_relay_conversion_package_fav_monthly",
                tags=["digital_customer_relay_conversion_package_fav_monthly"],
            ),
        ]
    )

########################################## Combine score category monthly  ###############################################
def digital_to_l3_combine_monthly_feature_score(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_combine_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_1",
                    "l3_digital_customer_web_category_favorite_monthly_catlv_1",
                    "params:l3_digital_customer_combine_score_sql_combine",
                    "params:l3_digital_customer_combine_score_sql_total",
                    "params:l3_digital_customer_combine_score_sql_share",
                    "params:l3_digital_customer_combine_score_sql",
                    "params:l3_digital_customer_combine_score_sql_sum"
                ],
                outputs="l3_digital_customer_combine_category_score_monthly_catlv_1",
                tags=["digital_customer_combine_category_score_monthly_catlv_1"],
            ),
            node(
                func=l3_digital_mobile_combine_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_2",
                    "l3_digital_customer_web_category_favorite_monthly_catlv_2",
                    "params:l3_digital_customer_combine_score_sql_combine",
                    "params:l3_digital_customer_combine_score_sql_total",
                    "params:l3_digital_customer_combine_score_sql_share",
                    "params:l3_digital_customer_combine_score_sql",
                    "params:l3_digital_customer_combine_score_sql_sum"
                ],
                outputs="l3_digital_customer_combine_category_score_monthly_catlv_2",
                tags=["digital_customer_combine_category_score_monthly_catlv_2"],
            ),
            node(
                func=l3_digital_mobile_combine_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_3",
                    "l3_digital_customer_web_category_favorite_monthly_catlv_3",
                    "params:l3_digital_customer_combine_score_sql_combine",
                    "params:l3_digital_customer_combine_score_sql_total",
                    "params:l3_digital_customer_combine_score_sql_share",
                    "params:l3_digital_customer_combine_score_sql",
                    "params:l3_digital_customer_combine_score_sql_sum"
                ],
                outputs="l3_digital_customer_combine_category_score_monthly_catlv_3",
                tags=["digital_customer_combine_category_score_monthly_catlv_3"],
            ),
            node(
                func=l3_digital_mobile_combine_category_score_monthly,
                inputs=[
                    "l3_digital_customer_app_category_favorite_monthly_catlv_4",
                    "l3_digital_customer_web_category_favorite_monthly_catlv_4",
                    "params:l3_digital_customer_combine_score_sql_combine",
                    "params:l3_digital_customer_combine_score_sql_total",
                    "params:l3_digital_customer_combine_score_sql_share",
                    "params:l3_digital_customer_combine_score_sql",
                    "params:l3_digital_customer_combine_score_sql_sum"
                ],
                outputs="l3_digital_customer_combine_category_score_monthly_catlv_4",
                tags=["digital_customer_combine_category_score_monthly_catlv_4"],
            ),

        ], name="digital_combine_monthly_feature_score_pipeline"
    )
########################################## Combine Favorite by category monthly  ###############################################
def digital_to_l3_combine_favorite_by_category_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_mobile_combine_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_combine_favorite_by_category_monthly_catlv_1",
                    "params:l3_digital_customer_combine_favorite_by_category_sql",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_volume",
                    "params:category_level_1"
                ],
                outputs="l3_digital_customer_combine_favorite_by_category_monthly_catlv_1",
                tags=["l3_digital_customer_combine_favorite_by_category_monthly_catlv_1"],
            ),
            node(
                func=l3_digital_mobile_combine_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_combine_favorite_by_category_monthly_catlv_2",
                    "params:l3_digital_customer_combine_favorite_by_category_sql",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_volume",
                    "params:category_level_2"
                ],
                outputs="l3_digital_customer_combine_favorite_by_category_monthly_catlv_2",
                tags=["l3_digital_customer_combine_favorite_by_category_monthly_catlv_2"],
            ),
            node(
                func=l3_digital_mobile_combine_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_combine_favorite_by_category_monthly_catlv_3",
                    "params:l3_digital_customer_combine_favorite_by_category_sql",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_volume",
                    "params:category_level_3"
                ],
                outputs="l3_digital_customer_combine_favorite_by_category_monthly_catlv_3",
                tags=["l3_digital_customer_combine_favorite_by_category_monthly_catlv_3"],
            ),
            node(
                func=l3_digital_mobile_combine_favorite_by_category_monthly,
                inputs=[
                    "l3_digital_customer_app_agg_monthly",
                    "l0_digital_mobile_web_monthly_for_l3_digital_customer_combine_favorite_by_category_monthly_catlv_4",
                    "params:l3_digital_customer_combine_favorite_by_category_sql",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_transection",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_duration",
                    "params:l3_digital_customer_combine_favorite_by_category_sql_volume",
                    "params:category_level_4"
                ],
                outputs="l3_digital_customer_combine_favorite_by_category_monthly_catlv_4",
                tags=["l3_digital_customer_combine_favorite_by_category_monthly_catlv_4"],
            ),
        ], name="digital_combine_monthly_feature_score_pipeline"
    )

########################################## Combine agg category timeband monthly  ###############################################
def digital_to_l3_combine_category_timeband_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_morning_catlv_1",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_1",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_1"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_1",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_1",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_1"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_evening_catlv_1",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_1",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_1"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_night_catlv_1",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_1",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_1"],
            ), #Combine agg category timeband monthly level 1
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_morning_catlv_2",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_2",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_2"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_2",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_2",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_2"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_evening_catlv_2",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_2",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_2"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_night_catlv_2",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_2",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_2"],
            ), #Combine agg category timeband monthly level 2
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_morning_catlv_3",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_3",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_3"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_3",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_3",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_3"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_evening_catlv_3",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_3",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_3"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_night_catlv_3",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_3",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_3"],
            ), #Combine agg category timeband monthly level 3
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_morning_catlv_4",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_4",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_4",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_4"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_4",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_4",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_4",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_4"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_evening_catlv_4",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_4",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_4",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_4"],
            ),
            node(
                func=digital_to_l3_digital_combine_timeband_monthly,
                inputs=[
                    "l1_digital_customer_combine_category_agg_timeband_night_catlv_4",
                    "l3_digital_customer_combine_category_agg_monthly_catlv_4",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql",
                    "params:l3_digital_customer_combine_agg_category_timeband_sql_share"
                ],
                outputs="l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_4",
                tags=["l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_4"],
            ), #Combine agg category timeband monthly level 4
        ], name="l3_digital_to_l3_combine_category_timeband_monthly"
    )

############### Cxense monthly  ###################
def digital_to_l3_cxense_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=l3_digital_cxense_category_agg_monthly,
                inputs=[
                    "l1_digital_cxense_traffic_complete_agg_daily_morning",
                    "params:l3_digital_cxense_category_agg_sql",
                ],
                outputs="l3_digital_cxense_category_agg_monthly_morning_catlv_1",
                tags=["l3_digital_cxense_category_agg_monthly_morning_catlv_1"],
            ),
            node(
                func=l3_digital_cxense_category_agg_monthly,
                inputs=[
                    "l1_digital_cxense_traffic_complete_agg_daily_afternoon",
                    "params:l3_digital_cxense_category_agg_sql",
                ],
                outputs="l3_digital_cxense_category_agg_monthly_afternoon_catlv_1",
                tags=["l3_digital_cxense_category_agg_monthly_afternoon_catlv_1"],
            ),
            node(
                func=l3_digital_cxense_category_agg_monthly,
                inputs=[
                    "l1_digital_cxense_traffic_complete_agg_daily_evening",
                    "params:l3_digital_cxense_category_agg_sql",
                ],
                outputs="l3_digital_cxense_category_agg_monthly_evening_catlv_1",
                tags=["l3_digital_cxense_category_agg_monthly_evening_catlv_1"],
            ),
            node(
                func=l3_digital_cxense_category_agg_monthly,
                inputs=[
                    "l1_digital_cxense_traffic_complete_agg_daily_night",
                    "params:l3_digital_cxense_category_agg_sql",
                ],
                outputs="l3_digital_cxense_category_agg_monthly_night_catlv_1",
                tags=["l3_digital_cxense_category_agg_monthly_night_catlv_1"],
            ),
        ], name="digital_to_l3_cxense_agg_monthly"
    )

def digital_to_l3_customer_multi_company_sim_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_multi_company_sim_monthly,
                inputs=[
                    "l1_digital_customer_web_network_company_usage_hourly_for_monthly",
                    "params:l3_customer_multi_company_sim"
                    ],
                outputs="l3_digital_customer_multi_company_sim_monthly",
                tags=["digital_customer_multi_company_sim_monthly"],
            ),
        ]
    )