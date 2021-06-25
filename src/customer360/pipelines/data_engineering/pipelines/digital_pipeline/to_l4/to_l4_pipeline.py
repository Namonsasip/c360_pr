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
from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l4.to_l4_nodes import *


def digital_to_l4_monthly_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l3_digital_cxenxse_user_profile_monthly",
                 "params:l4_digital_cxenzxse_user_profile_monthly_features"],
                "l4_digital_cxenzxse_user_profile_monthly_features"
            ),

        ], name="digital_to_l4_monthly_pipeline"
    )


def digital_to_l4_weekly_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_digital_cxenxse_site_traffic_weekly",
                 "params:l4_digital_cxenxse_site_traffic_weekly_features"],
                "l4_digital_cxenxse_site_traffic_weekly_features"
            ),

        ], name="digital_to_l4_weekly_pipeline"
    )


def digital_to_l4_weekly_favourite_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_digital_cxenxse_site_traffic_popular_host_weekly",
                 "params:l4_digital_cxenxse_site_traffic_popular_host_weekly_int"],
                "l4_digital_cxenxse_site_traffic_popular_host_weekly_features_int"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_digital_cxenxse_site_traffic_popular_host_weekly_features_int",
                 "params:l4_digital_cxenxse_site_traffic_popular_host_weekly_features"],
                "l4_digital_cxenxse_site_traffic_popular_host_weekly_features"
            ),
            node(
                l4_rolling_window,
                ["l2_digital_cxenxse_site_traffic_popular_postalcode_weekly",
                 "params:l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_int"],
                "l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_features_int"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_features_int",
                 "params:l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_features"],
                "l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_features"
            ),
            node(
                l4_rolling_window,
                ["l2_digital_cxenxse_site_traffic_popular_referrerquery_weekly",
                 "params:l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_int"],
                "l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_features_int"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_features_int",
                 "params:l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_features"],
                "l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_features"
            ),
            node(
                l4_rolling_window,
                ["l2_digital_cxenxse_site_traffic_popular_referrerhost_weekly",
                 "params:l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_int"],
                "l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_int"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_int",
                 "params:l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_features"],
                "l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_features"
            ),

        ], name="digital_to_l4_weekly_favourite_pipeline"
    )

def digital_to_l4_digital_customer_app_category_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_agg_monthly_for_l4_windows_app_category_agg_monthly",
                    "params:customer_app_category_monthly_groupby",
                    "params:customer_app_category_monthly_feature"
                ],
                outputs="l4_digital_customer_app_category_agg_monthly",
                tags=["l4_windows_app_category_agg_monthly"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_score_monthly_catlv_1",
                    "params:customer_app_score_monthly_groupby",
                    "params:customer_app_score_monthly_feature"
                ],
                outputs="l4_digital_customer_app_category_score_monthly_catlv_1",
                tags=["l4_digital_customer_app_category_score_monthly"],
            ),
        ], name="l4_digital_to_l4_digital_customer_app_category_monthly"
    )

def digital_to_l4_digital_customer_combine_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_1_for_l4_windows_combine_agg_monthly",
                    "params:customer_combine_agg_monthly_groupby",
                    "params:customer_combine_agg_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_monthly",
                tags=["l4_windows_app_combine_agg_monthly"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_2_for_l4_windows_combine_agg_monthly_catlv_2",
                    "params:customer_combine_agg_monthly_groupby",
                    "params:customer_combine_agg_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_monthly_catlv_2",
                tags=["l4_windows_app_combine_agg_monthly_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3_for_l4_windows_combine_agg_monthly_catlv_3",
                    "params:customer_combine_agg_monthly_groupby",
                    "params:customer_combine_agg_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_monthly_catlv_3",
                tags=["l4_windows_app_combine_agg_monthly_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_monthly_catlv_3_for_l4_windows_combine_agg_monthly_catlv_4",
                    "params:customer_combine_agg_monthly_groupby",
                    "params:customer_combine_agg_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_monthly_catlv_4",
                tags=["l4_windows_app_combine_agg_monthly_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_score_monthly_catlv_1",
                    "params:customer_combine_monthly_feature_score_groupby",
                    "params:customer_combine_monthly_feature_score_feature"
                ],
                outputs="l4_digital_customer_combine_category_score_monthly",
                tags=["l4_windows_app_combine_monthly_feature_score"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_score_monthly_catlv_2",
                    "params:customer_combine_monthly_feature_score_groupby",
                    "params:customer_combine_monthly_feature_score_feature"
                ],
                outputs="l4_digital_customer_combine_category_score_monthly_catlv_2",
                tags=["l4_windows_app_combine_monthly_feature_score_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_score_monthly_catlv_3",
                    "params:customer_combine_monthly_feature_score_groupby",
                    "params:customer_combine_monthly_feature_score_feature"
                ],
                outputs="l4_digital_customer_combine_category_score_monthly_catlv_3",
                tags=["l4_windows_app_combine_monthly_feature_score_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_score_monthly_catlv_4",
                    "params:customer_combine_monthly_feature_score_groupby",
                    "params:customer_combine_monthly_feature_score_feature"
                ],
                outputs="l4_digital_customer_combine_category_score_monthly_catlv_4",
                tags=["l4_windows_app_combine_monthly_feature_score_catlv_4"],
            ),
        ], name="l4_digital_to_l4_digital_customer_app_combine_agg_monthly"
    )

#web
def digital_to_l4_digital_customer_web_category_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_for_l4_digital_customer_web_category_agg_monthly",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_agg_monthly",
                tags=["l4_windows_web_category_agg_monthly"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_for_l4_digital_customer_web_category_agg_monthly_catlv_2",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_agg_monthly_catlv_2",
                tags=["l4_windows_web_category_agg_monthly_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_for_l4_digital_customer_web_category_agg_monthly_catlv_3",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_agg_monthly_catlv_3",
                tags=["l4_windows_web_category_agg_monthly_catlv_3"],
            ), 
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_monthly_for_l4_digital_customer_web_category_agg_monthly_catlv_4",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_agg_monthly_catlv_4",
                tags=["l4_windows_web_category_agg_monthly_catlv_4"],
            ),                        
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_score_monthly_catlv_1",
                    "params:customer_web_score_monthly_groupby",
                    "params:customer_web_score_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_score_monthly_catlv_1",
                tags=["l4_digital_customer_web_category_score_monthly"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_score_monthly_catlv_2",
                    "params:customer_web_score_monthly_groupby",
                    "params:customer_web_score_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_score_monthly_catlv_2",
                tags=["l4_digital_customer_web_category_score_monthly_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_score_monthly_catlv_3",
                    "params:customer_web_score_monthly_groupby",
                    "params:customer_web_score_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_score_monthly_catlv_3",
                tags=["l4_digital_customer_web_category_score_monthly_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_score_monthly_catlv_4",
                    "params:customer_web_score_monthly_groupby",
                    "params:customer_web_score_monthly_feature"
                ],
                outputs="l4_digital_customer_web_category_score_monthly_catlv_4",
                tags=["l4_digital_customer_web_category_score_monthly_catlv_4"],
            ),            
        ], name="digital_to_l4_digital_customer_web_category_monthly"
    )

def digital_to_l4_digital_customer_web_category_timeband_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_morning_catlv_1",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_morning_catlv_1",
                tags=["l4_windows_web_category_agg_timeband_monthly_morning"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_afternoon_catlv_1",
                tags=["l4_windows_web_category_agg_timeband_monthly_afternoon"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_evening_catlv_1",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_evening_catlv_1",
                tags=["l4_windows_web_category_agg_timeband_monthly_evening"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_night_catlv_1",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_night_catlv_1",
                tags=["l4_windows_web_category_agg_timeband_monthly_night"],
            ),
            ############## Category_level_2 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_morning_catlv_2",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_morning_catlv_2",
                tags=["l4_windows_web_category_agg_timeband_monthly_morning_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_afternoon_catlv_2",
                tags=["l4_windows_web_category_agg_timeband_monthly_afternoon_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_evening_catlv_2",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_evening_catlv_2",
                tags=["l4_windows_web_category_agg_timeband_monthly_evening_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_night_catlv_2",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_night_catlv_2",
                tags=["l4_windows_web_category_agg_timeband_monthly_night_catlv_2"],
            ),
            ############## Category_level_3 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_morning_catlv_3",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_morning_catlv_3",
                tags=["l4_windows_web_category_agg_timeband_monthly_morning_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_afternoon_catlv_3",
                tags=["l4_windows_web_category_agg_timeband_monthly_afternoon_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_evening_catlv_3",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_evening_catlv_3",
                tags=["l4_windows_web_category_agg_timeband_monthly_evening_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_night_catlv_3",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_night_catlv_3",
                tags=["l4_windows_web_category_agg_timeband_monthly_night_catlv_3"],
            ),      
            ############## Category_level_4 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_morning_catlv_4",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_morning_catlv_4",
                tags=["l4_windows_web_category_agg_timeband_monthly_morning_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_afternoon_catlv_4",
                tags=["l4_windows_web_category_agg_timeband_monthly_afternoon_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_evening_catlv_4",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_evening_catlv_4",
                tags=["l4_windows_web_category_agg_timeband_monthly_evening_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_web_category_agg_timeband_for_l4_digital_customer_web_category_agg_timeband_night_catlv_4",
                    "params:customer_web_category_monthly_groupby",
                    "params:customer_web_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_web_category_agg_timeband_night_catlv_4",
                tags=["l4_windows_web_category_agg_timeband_monthly_night_catlv_4"],
            ), 
        ], name="digital_to_l4_digital_customer_web_category_timeband_monthly"
    )

def digital_to_l4_customer_relay_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs= ["l3_digital_customer_relay_conversion_agg_monthly",
                 "params:relay_conversion_monthly_groupby",
                 "params:relay_conversion_monthly_feature"
                 ],
                outputs="l4_digital_customer_relay_conversion_agg_monthly",
                tags=["l4_windows_customer_relay_conversion_agg_monthly"],
            ),
             node(
                func=customer_category_windows,
                inputs=["l3_digital_customer_relay_pageview_fav_monthly",
                 "params:relay_pageview_monthly_groupby",
                 "params:relay_pageview_monthly_feature"
                 ],
                outputs="l4_digital_customer_relay_pageview_agg_monthly",
                tags=["l4_windows_customer_relay_pageview_agg_monthly"],
            ),

        ], name="digital_to_l4_customer_relay_monthly"
    )


def digital_to_l4_digital_customer_app_category_timeband_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_morning_catlv_1",
                    "params:customer_app_category_monthly_groupby",
                    "params:customer_app_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_app_category_agg_timeband_morning_catlv_1",
                tags=["l4_windows_app_category_agg_timeband_monthly_morning"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_afternoon_catlv_1",
                    "params:customer_app_category_monthly_groupby",
                    "params:customer_app_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_app_category_agg_timeband_afternoon_catlv_1",
                tags=["l4_windows_app_category_agg_timeband_monthly_afternoon"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_evening_catlv_1",
                    "params:customer_app_category_monthly_groupby",
                    "params:customer_app_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_app_category_agg_timeband_evening_catlv_1",
                tags=["l4_windows_app_category_agg_timeband_monthly_evening"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_app_category_agg_timeband_monthly_night_catlv_1",
                    "params:customer_app_category_monthly_groupby",
                    "params:customer_app_category_monthly_feature_timeband"
                ],
                outputs="l4_digital_customer_app_category_agg_timeband_night_catlv_1",
                tags=["l4_windows_app_category_agg_timeband_monthly_night"],
            ),
        ], name="digital_to_l4_digital_customer_app_category_timeband_monthly"
    )

def digital_to_l4_digital_customer_combine_category_timeband_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_1",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_1",
                tags=["l4_windows_combine_category_agg_timeband_monthly_morning_catlv_1"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_1",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_1",
                tags=["l4_windows_combine_category_agg_timeband_monthly_afternoon_catlv_1"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_1",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_1",
                tags=["l4_windows_combine_category_agg_timeband_monthly_evening_catlv_1"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_1",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_night_catlv_1",
                tags=["l4_windows_combine_category_agg_timeband_monthly_night_catlv_1"],
            ),
            ############## Category_level_2 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_2",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_2",
                tags=["l4_windows_combine_category_agg_timeband_monthly_morning_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_2",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_2",
                tags=["l4_windows_combine_category_agg_timeband_monthly_afternoon_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_2",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_2",
                tags=["l4_windows_combine_category_agg_timeband_monthly_evening_catlv_2"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_2",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_night_catlv_2",
                tags=["l4_windows_combine_category_agg_timeband_monthly_night_catlv_2"],
            ),         
            ############## Category_level_3 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_3",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_3",
                tags=["l4_windows_combine_category_agg_timeband_monthly_morning_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_3",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_3",
                tags=["l4_windows_combine_category_agg_timeband_monthly_afternoon_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_3",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_3",
                tags=["l4_windows_combine_category_agg_timeband_monthly_evening_catlv_3"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_3",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_night_catlv_3",
                tags=["l4_windows_combine_category_agg_timeband_monthly_night_catlv_3"],
            ), 
            ############## Category_level_4 ###################
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_4",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_morning_catlv_4",
                tags=["l4_windows_combine_category_agg_timeband_monthly_morning_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_4",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_afternoon_catlv_4",
                tags=["l4_windows_combine_category_agg_timeband_monthly_afternoon_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_4",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_evening_catlv_4",
                tags=["l4_windows_combine_category_agg_timeband_monthly_evening_catlv_4"],
            ),
            node(
                func=customer_category_windows,
                inputs=[
                    "l3_digital_customer_combine_category_agg_timeband_monthly_night_catlv_4",
                    "params:customer_combine_category_timeband_monthly_groupby",
                    "params:customer_combine_category_timeband_monthly_feature"
                ],
                outputs="l4_digital_customer_combine_category_agg_timeband_monthly_night_catlv_4",
                tags=["l4_windows_combine_category_agg_timeband_monthly_night_catlv_4"],
            ), 
        ], name="digital_to_l4_digital_customer_combine_category_timeband_monthly"
    )
