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
# from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_ranked_window


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

def digital_to_l4_digital_mobile_web_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l3_digital_customer_web_category_agg_monthly",
                 "params:l4_digital_digital_mobile_web_agg_features"],
                "l4_digital_digital_mobile_web_agg_features"
            ),

        ], name="digital_to_l4_digital_mobile_web_agg_monthly"
    )