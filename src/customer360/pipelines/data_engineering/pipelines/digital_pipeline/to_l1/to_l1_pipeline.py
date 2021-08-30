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

from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l1 import build_digital_l1_daily_features
from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l1.to_l1_nodes import *


def digital_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_digital_l1_daily_features,
                [
                 "l0_digital_cxenxse_site_traffic",
                 "l1_customer_profile_union_daily_feature_for_l1_digital_cxenxse_site_traffic_daily",
                 "params:exception_partition_list_for_l0_digital_cxenxse_site_traffic",
                 "params:l1_digital_cxenxse_site_traffic_daily",
                 "params:l1_digital_cxenxse_site_traffic_popular_host_daily",
                 "params:l1_digital_cxenxse_site_traffic_popular_postalcode_daily",
                 "params:l1_digital_cxenxse_site_traffic_popular_referrerquery_daily",
                 "params:l1_digital_cxenxse_site_traffic_popular_referrerhost_daily"
                 ],
                [
                 "l1_digital_cxenxse_site_traffic_daily",
                 "l1_digital_cxenxse_site_traffic_popular_host_daily",
                 "l1_digital_cxenxse_site_traffic_popular_postalcode_daily",
                 "l1_digital_cxenxse_site_traffic_popular_referrerquery_daily",
                 "l1_digital_cxenxse_site_traffic_popular_referrerhost_daily"
                ]
            ),
        ], name="digital_to_l1_pipeline"
    )
##################### Feature aib category master ###########################
def digital_to_l1_aib_categoy_clean_master(**kwargs):
    return Pipeline(
        [
            node(
                func=build_l1_digital_iab_category_table,
                inputs=["l0_digital_iab_categories_raw", "l0_digital_iab_category_priority_mapping"],
                outputs="l1_digital_aib_categories_clean",
                tags=["digital_l1_digital_aib_categories_clean"]
            ),
            node(
                func=digital_mobile_app_category_master,
                inputs=["l0_digital_app_master", "l0_digital_iab_categories_raw", "l0_digital_iab_category_priority_mapping"],
                outputs="l1_digital_app_category_master_clean",
                tags=["digital_mobile_app_category_master"],
            ),
        ],
        tags="digital_to_l1_aib_categoy_clean_master",
    )


######################## App category agg daily ##################################
def digital_to_l1_app_agg_daily_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_agg_daily,
                inputs=[
                 "l0_digital_mobile_app_daily_for_mobile_category_daily_catlv_1",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 "params:category_level_1",
                 ],
                outputs="l1_digital_customer_app_category_agg_daily_catlv_1",
                tags=["digital_mobile_app_category_agg_daily_catlv_1"],
            ),
            node(
                func=digital_mobile_app_category_agg_daily,
                inputs=[
                 "l0_digital_mobile_app_daily_for_mobile_category_daily_catlv_2",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 "params:category_level_2",
                 ],
                outputs="l1_digital_customer_app_category_agg_daily_catlv_2",
                tags=["digital_mobile_app_category_agg_daily_catlv_2"],
            ),
            node(
                func=digital_mobile_app_category_agg_daily,
                inputs=[
                 "l0_digital_mobile_app_daily_for_mobile_category_daily_catlv_3",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 "params:category_level_3",
                 ],
                outputs="l1_digital_customer_app_category_agg_daily_catlv_3",
                tags=["digital_mobile_app_category_agg_daily_catlv_3"],
            ),
            node(
                func=digital_mobile_app_category_agg_daily,
                inputs=[
                 "l0_digital_mobile_app_daily_for_mobile_category_daily_catlv_4",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 "params:category_level_4",
                 ],
                outputs="l1_digital_customer_app_category_agg_daily_catlv_4",
                tags=["digital_mobile_app_category_agg_daily_catlv_4"],
            ),
        ], name="digital_to_l1_app_agg_daily_pipeline"
    )
######################### App category agg category timeband daily ############################################
def digital_to_l1_app_agg_timeband_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_morning_catlv_1",
                "l1_digital_customer_app_category_agg_daily_for_share_morning_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Morning",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Morning"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_afternoon_catlv_1",
                "l1_digital_customer_app_category_agg_daily_for_share_afternoon_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Afternoon",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Afternoon"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_evening_catlv_1",
                "l1_digital_customer_app_category_agg_daily_for_share_evening_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Evening",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Evening"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_night_catlv_1",
                "l1_digital_customer_app_category_agg_daily_for_share_night_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Night",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Night"
            ), # app agg timeband level 1
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_morning_catlv_2",
                        "l1_digital_customer_app_category_agg_daily_for_share_morning_catlv_2",
                        "l1_digital_app_category_master_clean",
                        "params:level_2",
                        "params:timeband_Morning",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_2",
                tags="digital_mobile_app_category_agg_timeband_Morning_catlv_2"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_afternoon_catlv_2",
                        "l1_digital_customer_app_category_agg_daily_for_share_afternoon_catlv_2",
                        "l1_digital_app_category_master_clean",
                        "params:level_2",
                        "params:timeband_Afternoon",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_2",
                tags="digital_mobile_app_category_agg_timeband_Afternoon_catlv_2"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_evening_catlv_2",
                        "l1_digital_customer_app_category_agg_daily_for_share_evening_catlv_2",
                        "l1_digital_app_category_master_clean",
                        "params:level_2",
                        "params:timeband_Evening",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_2",
                tags="digital_mobile_app_category_agg_timeband_Evening_catlv_2"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_night_catlv_2",
                        "l1_digital_customer_app_category_agg_daily_for_share_night_catlv_2",
                        "l1_digital_app_category_master_clean",
                        "params:level_2",
                        "params:timeband_Night",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_2",
                tags="digital_mobile_app_category_agg_timeband_Night_catlv_2"
            ), # app agg timeband level 2
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_morning_catlv_3",
                        "l1_digital_customer_app_category_agg_daily_for_share_morning_catlv_3",
                        "l1_digital_app_category_master_clean",
                        "params:level_3",
                        "params:timeband_Morning",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_3",
                tags="digital_mobile_app_category_agg_timeband_Morning_catlv_3"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_afternoon_catlv_3",
                        "l1_digital_customer_app_category_agg_daily_for_share_afternoon_catlv_3",
                        "l1_digital_app_category_master_clean",
                        "params:level_3",
                        "params:timeband_Afternoon",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_3",
                tags="digital_mobile_app_category_agg_timeband_Afternoon_catlv_3"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_evening_catlv_3",
                        "l1_digital_customer_app_category_agg_daily_for_share_evening_catlv_3",
                        "l1_digital_app_category_master_clean",
                        "params:level_3",
                        "params:timeband_Evening",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_3",
                tags="digital_mobile_app_category_agg_timeband_Evening_catlv_3"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_night_catlv_3",
                        "l1_digital_customer_app_category_agg_daily_for_share_night_catlv_3",
                        "l1_digital_app_category_master_clean",
                        "params:level_3",
                        "params:timeband_Night",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_3",
                tags="digital_mobile_app_category_agg_timeband_Night_catlv_3"
            ), # app agg timeband level 3
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_morning_catlv_4",
                        "l1_digital_customer_app_category_agg_daily_for_share_morning_catlv_4",
                        "l1_digital_app_category_master_clean",
                        "params:level_4",
                        "params:timeband_Morning",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_4",
                tags="digital_mobile_app_category_agg_timeband_Morning_catlv_4"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_afternoon_catlv_4",
                        "l1_digital_customer_app_category_agg_daily_for_share_afternoon_catlv_4",
                        "l1_digital_app_category_master_clean",
                        "params:level_4",
                        "params:timeband_Afternoon",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_4",
                tags="digital_mobile_app_category_agg_timeband_Afternoon_catlv_4"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_evening_catlv_4",
                        "l1_digital_customer_app_category_agg_daily_for_share_evening_catlv_4",
                        "l1_digital_app_category_master_clean",
                        "params:level_4",
                        "params:timeband_Evening",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_4",
                tags="digital_mobile_app_category_agg_timeband_Evening_catlv_4"
            ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly_for_night_catlv_4",
                        "l1_digital_customer_app_category_agg_daily_for_share_night_catlv_4",
                        "l1_digital_app_category_master_clean",
                        "params:level_4",
                        "params:timeband_Night",
                        "params:l1_digital_mobile_app_agg_category_timeband",
                        "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_4",
                tags="digital_mobile_app_category_agg_timeband_Night_catlv_4"
            ), # app agg timeband level 4
        ],
        tags="digital_to_l1_app_agg_timeband_pipeline",
    )

##################### Web agg category daily ###########################
def digital_to_l1_digital_mobile_web_agg_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_customer_web_category_agg_union_daily,
                inputs=
                [
                    "l0_digital_mobile_web_daily_for_l1_digital_customer_web_category_agg_daily_catlv_1",
                    "l1_digital_cxense_traffic_complete_agg_daily_for_l1_dital_customer_web_agg_daily_catlv_1",
                    "l1_digital_aib_categories_clean",
                    "params:category_level_1",
                    "params:l1_dital_customer_web_agg_daily_sql"
                ],
                outputs="l1_digital_customer_web_category_agg_daily_catlv_1",
                tags="l1_digital_customer_web_category_agg_union_daily_catlv_1"
            ),
            node(
                func=l1_digital_customer_web_category_agg_union_daily,
                inputs=
                [
                    "l0_digital_mobile_web_daily_for_l1_digital_customer_web_category_agg_daily_catlv_2",
                    "l1_digital_cxense_traffic_complete_agg_daily_for_l1_dital_customer_web_agg_daily_catlv_2",
                    "l1_digital_aib_categories_clean",
                    "params:category_level_2",
                    "params:l1_dital_customer_web_agg_daily_sql"
                ],
                outputs="l1_digital_customer_web_category_agg_daily_catlv_2",
                tags="l1_digital_customer_web_category_agg_union_daily_catlv_2"
            ),
            node(
                func=l1_digital_customer_web_category_agg_union_daily,
                inputs=
                [
                    "l0_digital_mobile_web_daily_for_l1_digital_customer_web_category_agg_daily_catlv_3",
                    "l1_digital_cxense_traffic_complete_agg_daily_for_l1_dital_customer_web_agg_daily_catlv_3",
                    "l1_digital_aib_categories_clean",
                    "params:category_level_3",
                    "params:l1_dital_customer_web_agg_daily_sql"
                ],
                outputs="l1_digital_customer_web_category_agg_daily_catlv_3",
                tags="l1_digital_customer_web_category_agg_union_daily_catlv_3"
            ),
            node(
                func=l1_digital_customer_web_category_agg_union_daily,
                inputs=
                [
                    "l0_digital_mobile_web_daily_for_l1_digital_customer_web_category_agg_daily_catlv_4",
                    "l1_digital_cxense_traffic_complete_agg_daily_for_l1_dital_customer_web_agg_daily_catlv_4",
                    "l1_digital_aib_categories_clean",
                    "params:category_level_4",
                    "params:l1_dital_customer_web_agg_daily_sql"
                ],
                outputs="l1_digital_customer_web_category_agg_daily_catlv_4",
                tags="l1_digital_customer_web_category_agg_union_daily_catlv_4"
            ),
        ], tags="digital_to_l1_digital_mobile_web_agg_daily",
    )

##################### Web agg category daily timeband ###########################
def digital_to_l1_digital_mobile_web_agg_timeband(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_customer_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_morning_catlv_1",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_morning_catlv_1",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_morning",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_morning_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Morning",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_afternoon_catlv_1",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_afternoon_catlv_1",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_afternoon",
                    "params:l1_digital_mobile_web_timeband_sql_share"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Afternoon",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_evening_catlv_1",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_evening_catlv_1",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_evening",
                    "params:l1_digital_mobile_web_timeband_sql_share"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_evening_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Evening",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_night_catlv_1",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_night_catlv_1",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_night",
                    "params:l1_digital_mobile_web_timeband_sql_share"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_night_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Night",
            ), # Web agg timeband level 1
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_morning_catlv_2",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_morning_catlv_2",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_morning",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_2",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_morning_catlv_2",
                tags="l1_digital_mobile_web_category_agg_timeband_Morning_catlv_2",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_afternoon_catlv_2",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_afternoon_catlv_2",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_afternoon",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_2",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                tags="l1_digital_mobile_web_category_agg_timeband_Afternoon_catlv_2",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_evening_catlv_2",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_evening_catlv_2",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_evening",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_2",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_evening_catlv_2",
                tags="l1_digital_mobile_web_category_agg_timeband_Evening_catlv_2",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_night_catlv_2",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_night_catlv_2",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_night",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_2",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_night_catlv_2",
                tags="l1_digital_mobile_web_category_agg_timeband_Night_catlv_2",
            ), # Web agg timeband level 2
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_morning_catlv_3",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_morning_catlv_3",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_morning",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_3",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_morning_catlv_3",
                tags="l1_digital_mobile_web_category_agg_timeband_Morning_catlv_3",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_afternoon_catlv_3",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_afternoon_catlv_3",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_afternoon",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_3",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                tags="l1_digital_mobile_web_category_agg_timeband_Afternoon_catlv_3",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_evening_catlv_3",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_evening_catlv_3",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_evening",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_3",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_evening_catlv_3",
                tags="l1_digital_mobile_web_category_agg_timeband_Evening_catlv_3",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_night_catlv_3",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_night_catlv_3",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_night",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_3",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_night_catlv_3",
                tags="l1_digital_mobile_web_category_agg_timeband_Night_catlv_3",
            ), # Web agg timeband level 3
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_morning_catlv_4",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_morning_catlv_4",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_morning",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_4",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_morning_catlv_4",
                tags="l1_digital_mobile_web_category_agg_timeband_Morning_catlv_4",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_afternoon_catlv_4",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_afternoon_catlv_4",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_afternoon",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_4",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                tags="l1_digital_mobile_web_category_agg_timeband_Afternoon_catlv_4",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_evening_catlv_4",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_evening_catlv_4",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_evening",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_4",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_evening_catlv_4",
                tags="l1_digital_mobile_web_category_agg_timeband_Evening_catlv_4",
            ),
            node(
                func=l1_digital_customer_web_category_agg_timeband_cat_level,
                inputs=
                [
                    "l0_digital_mobile_web_hourly_for_night_catlv_4",
                    "l0_digital_customer_profile_union_daily",
                    "l1_digital_customer_web_category_agg_daily_for_night_catlv_4",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_sql",
                    "params:timeband_web_night",
                    "params:l1_digital_mobile_web_timeband_sql_share",
                    "params:level_4",
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_night_catlv_4",
                tags="l1_digital_mobile_web_category_agg_timeband_Night_catlv_4",
            ), # Web agg timeband level 4
        ], tags="digital_to_l1_digital_mobile_web_agg_timeband_morning",
    )

def digital_to_l1_customer_relay_agg_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_relay_pageview_agg_daily,
                inputs=["l0_digital_relay_engagement_pageview",
                        "params:l1_digital_relay_engagement_pageviews_count_visit",
                        ],
                outputs="l1_digital_customer_relay_pageview_agg_daily",
                tags=["digital_customer_relay_pageview_agg_daily"],
            ),
            node(
                func=digital_customer_relay_conversion_agg_daily,
                inputs = ["l0_digital_relay_engagement_conversion",
                          "l0_digital_relay_engagement_conversion_package",
                          "params:l1_digital_relay_engagement_conversion_count_visit_by_cid",
                          "params:l1_digital_relay_engagement_conversion_package_count_visit_by_cid",
                          ],
                outputs = "l1_digital_customer_relay_conversion_agg_daily",
                tags = ["digital_customer_relay_conversion_agg_daily"],
            ),

        ]
    )

#####################  Cxense content profile ###########################
def digital_to_l1_cxense_content_profile(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_cxense_content_profile_int,
                inputs="l0_digital_cxense_content_profile_raw",
                outputs="l1_digital_cxense_content_profile_int",
                tags="l1_digital_cxense_traffic_mapping",
            ),
            node(
                func=create_content_profile_mapping,
                inputs=["l1_digital_cxense_content_profile_int", "l1_digital_aib_categories_clean"],
                outputs="l1_digital_cxense_content_profile_mapping",
                tags="create_content_profile_mapping",
            ),
        ],tags="digital_to_l1_cxense_content_profile",
    )

#####################  Cxense agg category daily ###########################
def digital_to_l1_cxense_traffic_daily_agg_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_cxense_traffic_clean,
                inputs="l0_digital_cxense_traffic_raw",
                outputs="l1_digital_cxense_traffic_int",
                tags="l1_digital_cxense_traffic_mapping"
            ),
            node(
                func=l1_digital_agg_cxense_traffic,
                inputs="l1_digital_cxense_traffic_int",
                outputs="l1_digital_cxense_traffic_agg_daily",
                tags="l1_digital_agg_cxense_traffic"
            ),
            node(
                func=l1_digital_union_matched_and_unmatched_urls,
                inputs=
                [
                    "l1_digital_cxense_traffic_agg_daily",
                    "l1_digital_cxense_content_profile_mapping",
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_1"
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_1",
                tags="l1_digital_union_matched_and_unmatched_urls_catlv_1"
            ),
            node(
                func=l1_digital_union_matched_and_unmatched_urls_cat_level,
                inputs=
                [
                    "l1_digital_cxense_traffic_agg_daily",
                    "l1_digital_cxense_content_profile_mapping",
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_2",
                    "params:level_2"
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_2",
                tags="l1_digital_union_matched_and_unmatched_urls_catlv_2"
            ),
            node(
                func=l1_digital_union_matched_and_unmatched_urls_cat_level,
                inputs=
                [
                    "l1_digital_cxense_traffic_agg_daily",
                    "l1_digital_cxense_content_profile_mapping",
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_3",
                    "params:level_3"
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_3",
                tags="l1_digital_union_matched_and_unmatched_urls_catlv_3"
            ),
            node(
                func=l1_digital_union_matched_and_unmatched_urls_cat_level,
                inputs=
                [
                    "l1_digital_cxense_traffic_agg_daily",
                    "l1_digital_cxense_content_profile_mapping",
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_4",
                    "params:level_4"
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_4",
                tags="l1_digital_union_matched_and_unmatched_urls_catlv_4"
            ),
        ], tags="digital_to_l1_cxense_traffic_daily_agg_pipeline"
    )

def digital_to_l1_cxense_traffic_daily_agg_non_site_id_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_union_matched_and_unmatched_urls_non_site_id,
                inputs=
                [
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_1",
                    "l1_digital_best_match_for_unmatched_urls_for_union_matched_and_unmatched_urls_catlv_1"
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_1",
                tags=["l1_digital_union_matched_and_unmatched_urls_catlv_1"],
            ),
            # node(
            #     func=l1_digital_union_matched_and_unmatched_urls_non_site_id_cat_level,
            #     inputs=
            #     [
            #         "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_2",
            #         "l1_digital_best_match_for_unmatched_urls_for_union_matched_and_unmatched_urls_catlv_2",
            #         "params:level_2",
            #     ],
            #     outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_2",
            #     tags="l1_digital_union_matched_and_unmatched_urls_catlv_2",
            # ),
            node(
                func=l1_digital_union_matched_and_unmatched_urls_non_site_id_cat_level,
                inputs=
                [
                    "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_3",
                    "l1_digital_best_match_for_unmatched_urls_for_union_matched_and_unmatched_urls_catlv_3",
                    "params:level_3",
                ],
                outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_3",
                tags="l1_digital_union_matched_and_unmatched_urls_catlv_3",
            ),
            # node(
            #     func=l1_digital_union_matched_and_unmatched_urls_non_site_id_cat_level,
            #     inputs=
            #     [
            #         "l0_digital_customer_profile_union_daily_for_cxense_daily_catlv_4",
            #         "l1_digital_best_match_for_unmatched_urls_for_union_matched_and_unmatched_urls_catlv_4",
            #         "params:level_4",
            #     ],
            #     outputs="l1_digital_cxense_traffic_complete_agg_daily_catlv_4",
            #     tags="l1_digital_union_matched_and_unmatched_urls_catlv_4",
            # ),
        ],tags="digital_to_l1_cxense_traffic_daily_agg_non_site_id_pipeline",
    )

########################### Combine agg category daily ##########################
def digital_to_l1_combine_app_web_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_to_l1_combine_app_web_agg_daily,
                inputs=["l1_digital_customer_app_category_agg_daily_catlv_1_for_combine_daily_catlv_1",
                        "l1_digital_customer_web_category_agg_daily_catlv_1_for_combine_daily_catlv_1",
                        "params:l1_digital_customer_combine_app_web_agg_daily",
                        ],
                outputs="l1_digital_customer_combine_category_agg_daily_catlv_1",
                tags=["l1_digital_customer_combine_category_agg_daily_catlv_1"],
            ),
            node(
                func=digital_to_l1_combine_app_web_agg_daily,
                inputs=["l1_digital_customer_app_category_agg_daily_catlv_2_for_combine_daily_catlv_2",
                        "l1_digital_customer_web_category_agg_daily_catlv_2_for_combine_daily_catlv_2",
                        "params:l1_digital_customer_combine_app_web_agg_daily",
                        ],
                outputs="l1_digital_customer_combine_category_agg_daily_catlv_2",
                tags=["l1_digital_customer_combine_category_agg_daily_catlv_2"],
            ),
            node(
                func=digital_to_l1_combine_app_web_agg_daily,
                inputs=["l1_digital_customer_app_category_agg_daily_catlv_3_for_combine_daily_catlv_3",
                        "l1_digital_customer_web_category_agg_daily_catlv_3_for_combine_daily_catlv_3",
                        "params:l1_digital_customer_combine_app_web_agg_daily",
                        ],
                outputs="l1_digital_customer_combine_category_agg_daily_catlv_3",
                tags=["l1_digital_customer_combine_category_agg_daily_catlv_3"],
            ),
            node(
                func=digital_to_l1_combine_app_web_agg_daily,
                inputs=["l1_digital_customer_app_category_agg_daily_catlv_4_for_combine_daily_catlv_4",
                        "l1_digital_customer_web_category_agg_daily_catlv_4_for_combine_daily_catlv_4",
                        "params:l1_digital_customer_combine_app_web_agg_daily",
                        ],
                outputs="l1_digital_customer_combine_category_agg_daily_catlv_4",
                tags=["l1_digital_customer_combine_category_agg_daily_catlv_4"],
            ),

        ],tags="digital_to_l1_combine_app_web_daily",
    )

 ################## Combine agg category timeband daily ######################
def digital_to_l1_digital_mobile_combine_agg_timeband(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_morning_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_web_category_agg_timeband_morning_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_combine_category_agg_daily_catlv_1",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_morning_catlv_1",
                tags="l1_digital_mobile_combine_category_agg_timeband_morning_catlv_1",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_afternoon_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_web_category_agg_timeband_afternoon_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_combine_category_agg_daily_catlv_1",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_1",
                tags="l1_digital_mobile_combine_category_agg_timeband_afternoon_catlv_1",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_evening_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_web_category_agg_timeband_evening_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_combine_category_agg_daily_catlv_1",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_evening_catlv_1",
                tags="l1_digital_mobile_combine_category_agg_timeband_evening_catlv_1",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_night_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_web_category_agg_timeband_night_catlv_1_for_combine_timeband_catlv_1",
                    "l1_digital_customer_combine_category_agg_daily_catlv_1",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_night_catlv_1",
                tags="l1_digital_mobile_combine_category_agg_timeband_night_catlv_1",
            ),

            ############## Category_level_2 ###################
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_morning_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_web_category_agg_timeband_morning_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_combine_category_agg_daily_catlv_2",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_morning_catlv_2",
                tags="l1_digital_mobile_combine_category_agg_timeband_morning_catlv_2",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_afternoon_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_web_category_agg_timeband_afternoon_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_combine_category_agg_daily_catlv_2",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_2",
                tags="l1_digital_mobile_combine_category_agg_timeband_afternoon_catlv_2",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_evening_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_web_category_agg_timeband_evening_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_combine_category_agg_daily_catlv_2",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_evening_catlv_2",
                tags="l1_digital_mobile_combine_category_agg_timeband_evening_catlv_2",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_night_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_web_category_agg_timeband_night_catlv_2_for_combine_timeband_catlv_2",
                    "l1_digital_customer_combine_category_agg_daily_catlv_2",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_night_catlv_2",
                tags="l1_digital_mobile_combine_category_agg_timeband_night_catlv_2",
            ),


            ############## Category_level_3 ###################
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_morning_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_web_category_agg_timeband_morning_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_combine_category_agg_daily_catlv_3",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_morning_catlv_3",
                tags="l1_digital_mobile_combine_category_agg_timeband_morning_catlv_3",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_afternoon_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_web_category_agg_timeband_afternoon_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_combine_category_agg_daily_catlv_3",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_3",
                tags="l1_digital_mobile_combine_category_agg_timeband_afternoon_catlv_3",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_evening_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_web_category_agg_timeband_evening_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_combine_category_agg_daily_catlv_3",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_evening_catlv_3",
                tags="l1_digital_mobile_combine_category_agg_timeband_evening_catlv_3",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_night_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_web_category_agg_timeband_night_catlv_3_for_combine_timeband_catlv_3",
                    "l1_digital_customer_combine_category_agg_daily_catlv_3",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_night_catlv_3",
                tags="l1_digital_mobile_combine_category_agg_timeband_night_catlv_3",
            ),

            ############## Category_level_4 ###################
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_morning_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_web_category_agg_timeband_morning_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_combine_category_agg_daily_catlv_4",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_morning_catlv_4",
                tags="l1_digital_mobile_combine_category_agg_timeband_morning_catlv_4",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_afternoon_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_web_category_agg_timeband_afternoon_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_combine_category_agg_daily_catlv_4",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_afternoon_catlv_4",
                tags="l1_digital_mobile_combine_category_agg_timeband_afternoon_catlv_4",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_evening_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_web_category_agg_timeband_evening_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_combine_category_agg_daily_catlv_4",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_evening_catlv_4",
                tags="l1_digital_mobile_combine_category_agg_timeband_evening_catlv_4",
            ),
            node(
                func=l1_digital_customer_combine_category_agg_timeband,
                inputs=
                [
                    "l1_digital_customer_app_category_agg_timeband_night_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_web_category_agg_timeband_night_catlv_4_for_combine_timeband_catlv_4",
                    "l1_digital_customer_combine_category_agg_daily_catlv_4",
                    "params:l1_digital_customer_combine_app_web_agg_timeband",
                    "params:l1_digital_customer_combine_app_web_agg_timeband_sql_share"
                ],
                outputs="l1_digital_customer_combine_category_agg_timeband_night_catlv_4",
                tags="l1_digital_mobile_combine_category_agg_timeband_night_catlv_4",
            ),

        ], tags="l1_digital_to_l1_digital_mobile_combine_agg_timeband",
    )

def digital_to_l1_digital_cxense_user_traffic(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_cxense_traffic_mapping_subscription_identifier,
                inputs=[
                    "l0_digital_cxense_user_traffic",
                    "l1_digital_union_daily_feature_for_cxense_user_traffic"
                    ],
                outputs="l1_digital_cxense_user_traffic",
                tags=["digital_cxense_traffic_mapping_subscription_identifier"],
            ),
        ]
    )

def digital_to_l1_customer_web_network_company(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_web_network_company_usage_hourly,
                inputs=[
                    "l1_digital_cxense_user_traffic"
                    ],

                outputs="l1_digital_customer_web_network_company_usage_hourly",
                tags=["digital_customer_web_network_company_usage_hourly"],
            ),
        ]
    )

def digital_to_l1_customer_multi_company_sim_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_multi_company_sim_daily,
                inputs=[
                    "l1_digital_customer_web_network_company_usage_hourly",
                    "params:l1_customer_multi_company_sim"
                    ],
                outputs="l1_digital_customer_multi_company_sim_daily",
                tags=["digital_customer_multi_company_sim_daily"],
            ),
        ]
    )