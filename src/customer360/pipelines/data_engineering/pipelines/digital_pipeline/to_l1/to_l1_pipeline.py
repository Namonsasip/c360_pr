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
##################### Feature 2021-05 ###########################
def digital_to_l1_aib_categoy_clean_master(**kwargs):
    return Pipeline(
        [
            node(
                func=build_l1_digital_iab_category_table,
                inputs=["l0_digital_iab_categories_raw", "l0_digital_iab_category_priority_mapping"],
                outputs="l1_digital_aib_categories_clean"
            ),
            # node(
            #     func=digital_mobile_app_category_master,
            #     inputs=["l0_digital_app_master", "l0_digital_iab_categories_raw", "l0_digital_iab_category_priority_mapping"],
            #     outputs="l1_digital_app_category_master_clean",
            #     tags=["digital_mobile_app_category_master"],
            # ),
        ],
        tags="digital_to_l1_aib_categoy_clean_master",
    )

def digital_to_l1_app_agg_daily_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_agg_daily,
                inputs=[
                 "l0_digital_mobile_app_daily",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 "params:category_level_1",
                 ],
                outputs="l1_digital_customer_app_category_agg_daily_catlv_1",
                tags=["digital_mobile_app_category_agg_daily"],
            ),
        ], name="digital_to_l1_app_agg_daily_pipeline"
    )

def digital_to_l1_app_agg_timeband_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly",
                "l1_digital_customer_app_category_agg_daily_for_share_timeband_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Morning",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Morning"
            ),
            # node(
            #     func=digital_mobile_app_category_agg_timeband_feature,
            #     inputs=["l1_digital_mobile_app_category_agg_timeband_morning_catlv_1",
            #     "l1_digital_union_daily_feature_for_mobile_app_catefory_timeband"],
            #     outputs="l1_digital_customer_app_category_agg_timeband_morning_catlv_1",
            #     tags="digital_mobile_app_category_agg_timeband_feature_Morning"
            # ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly",
                "l1_digital_customer_app_category_agg_daily_for_share_timeband_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Afternoon",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Afternoon"
            ),
            # node(
            #     func=digital_mobile_app_category_agg_timeband_feature,
            #     inputs=["l1_digital_mobile_app_category_agg_timeband_afternoon_catlv_1",
            #     "l1_digital_union_daily_feature_for_mobile_app_catefory_timeband"],
            #     outputs="l1_digital_customer_app_category_agg_timeband_afternoon_catlv_1",
            #     tags="digital_mobile_app_category_agg_timeband_feature_Afternoon"
            # ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly",
                "l1_digital_customer_app_category_agg_daily_for_share_timeband_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Evening",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Evening"
            ),
            # node(
            #     func=digital_mobile_app_category_agg_timeband_feature,
            #     inputs=["l1_digital_mobile_app_category_agg_timeband_evening_catlv_1",
            #     "l1_digital_union_daily_feature_for_mobile_app_catefory_timeband"],
            #     outputs="l1_digital_customer_app_category_agg_timeband_evening_catlv_1",
            #     tags="digital_mobile_app_category_agg_timeband_feature_Evening"
            # ),
            node(
                func=digital_mobile_app_category_agg_timeband,
                inputs=["l0_digital_app_hourly",
                "l1_digital_customer_app_category_agg_daily_for_share_timeband_catlv_1",
                "l1_digital_app_category_master_clean",
                "params:level_1",
                "params:timeband_Night",
                "params:l1_digital_mobile_app_agg_category_timeband",
                "params:l1_digital_mobile_app_timeband_sql_share"],
                outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_1",
                tags="digital_mobile_app_category_agg_timeband_Night"
            ),
            # node(
            #     func=digital_mobile_app_category_agg_timeband_feature,
            #     inputs=["l1_digital_mobile_app_category_agg_timeband_night_catlv_1",
            #     "l1_digital_union_daily_feature_for_mobile_app_catefory_timeband"],
            #     outputs="l1_digital_customer_app_category_agg_timeband_night_catlv_1",
            #     tags="digital_mobile_app_category_agg_timeband_feature_Night"
            # ),
        ],
        tags="digital_to_l1_app_agg_timeband_pipeline",
    )


def digital_to_l1_digital_mobile_web_agg_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_customer_web_category_agg_daily,
                inputs=["l0_digital_mobile_web_daily", "l1_digital_aib_categories_clean"],
                outputs="l1_digital_customer_web_category_agg_daily",
                tags="l1_digital_customer_web_category_agg_daily"
            ),
            # node(
            #     func=l1_digital_mobile_web_level_category,
            #     inputs="l1_digital_customer_web_category_agg_daily",
            #     outputs="l1_digital_mobile_web_level_stats",
            #     tags=["l1_digital_mobile_web_level_stats"],
            # ),
        ],tags="digital_to_l1_digital_mobile_web_agg_daily",
    )

def digital_to_l1_digital_mobile_web_agg_timeband(**kwargs):
    return Pipeline(
        [
            node(
                func=l1_digital_mobile_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_morning_catlv1",
                    "params:timeband_web_morning"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_morning_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Morning",
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband_features,
                inputs=
                [
                    "l1_digital_union_daily_feature_for_mobile_web_category_timeband",
                    "l1_digital_customer_web_category_agg_timeband_morning_catlv_1",
                ],
                outputs="l1_digital_mobile_web_category_agg_timeband_morning_catlv_1",
                tags=["l1_digital_mobile_web_category_agg_timeband_features_Morning"],
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_afternoon_catlv1",
                    "params:timeband_web_afternoon"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Afternoon",
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband_features,
                inputs=
                [
                    "l1_digital_union_daily_feature_for_mobile_web_category_timeband",
                    "l1_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                ],
                outputs="l1_digital_mobile_web_category_agg_timeband_afternoon_catlv_1",
                tags=["l1_digital_mobile_web_category_agg_timeband_features_Afternoon"],
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_evening_catlv1",
                    "params:timeband_web_evening"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_evening_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Evening",
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband_features,
                inputs=
                [
                    "l1_digital_union_daily_feature_for_mobile_web_category_timeband",
                    "l1_digital_customer_web_category_agg_timeband_evening_catlv_1",
                ],
                outputs="l1_digital_mobile_web_category_agg_timeband_evening_catlv_1",
                tags=["l1_digital_mobile_web_category_agg_timeband_features_Evening"],
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband,
                inputs=
                [
                    "l0_digital_mobile_web_hourly",
                    "l1_digital_aib_categories_clean",
                    "params:l1_digital_mobile_web_agg_category_timeband_night_catlv1",
                    "params:timeband_web_night"
                ],
                outputs="l1_digital_customer_web_category_agg_timeband_night_catlv_1",
                tags="l1_digital_mobile_web_category_agg_timeband_Night",
            ),
            node(
                func=l1_digital_mobile_web_category_agg_timeband_features,
                inputs=
                [
                    "l1_digital_union_daily_feature_for_mobile_web_category_timeband",
                    "l1_digital_customer_web_category_agg_timeband_night_catlv_1",
                ],
                outputs="l1_digital_mobile_web_category_agg_timeband_night_catlv_1",
                tags=["l1_digital_mobile_web_category_agg_timeband_features_Night"],
            ),
        ],tags="digital_to_l1_digital_mobile_web_agg_timeband_morning",
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
            # node(
            #     func=digital_customer_relay_conversion_agg_daily,
            #     inputs = ["l0_digital_relay_engagement_conversion",
            #               "l0_digital_relay_engagement_conversion_package",
            #               "params:l1_digital_relay_engagement_conversion_count_visit_by_cid",
            #               "params:l1_digital_relay_engagement_conversion_package_count_visit_by_cid",
            #               ],
            #     outputs = "l1_digital_customer_relay_conversion_agg_daily",
            #     tags = ["digital_customer_relay_conversion_agg_daily"],
            # ),

        ]
    )

def digital_to_l1_combine_app_web_daily(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_to_l1_combine_app_web_agg_daily,
                inputs = ["l1_digital_customer_web_category_agg_daily",
                          "l1_digital_customer_web_category_agg_daily",
                          "params:l1_digital_customer_combine_app_web_agg_daily",
                          ],
                outputs = "l1_digital_customer_combine_category_agg_daily_catlv_1",
                tags = ["l1_digital_customer_combine_category_agg_daily_catlv_1"],
            ),

        ],tags="digital_to_l1_combine_app_web_daily",
    )