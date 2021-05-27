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

def digital_to_l1_app_agg_daily_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                digital_mobile_app_category_agg_daily,
                [
                 "l0_digital_mobile_app_daily",
                 "params:l1_digital_mobile_app_agg_category_daily",
                 ],
                [
                 "l1_digital_mobile_app_agg_category_daily_catlv_1",
                ]
            ),
        ], name="digital_to_l1_app_agg_daily_pipeline"
    )

def digital_to_l1_aib_categoy_clean_master(**kwargs):
    return Pipeline(
        [
            node(
                build_l1_digital_iab_category_table,
                [
                    "l0_iab_categories_raw",
                    "l0_iab_category_priority_mapping"
                ],
                [
                    "l1_digital_aib_categories_clean",
                ]
            ),
            # node(
            #     build_l1_digital_iab_category_table_catlv_2,
            #     [
            #         "l0_iab_categories_raw", "l0_iab_category_priority_mapping"
            #     ],
            #     [
            #         "l1_digital_aib_categories_clean_catlv_2"
            #     ],
            # ),
            # node(
            #     build_l1_digital_iab_category_table_catlv_3,
            #     [
            #         "l0_iab_categories_raw", "l0_iab_category_priority_mapping"
            #     ],
            #     [
            #         "l1_digital_aib_categories_clean_catlv_3"
            #     ],
            # ),
            # node(
            #     build_l1_digital_iab_category_table_catlv_4,
            #     [
            #         "l0_iab_categories_raw", "l0_iab_category_priority_mapping"
            #     ],
            #     [
            #         "l1_digital_aib_categories_clean_catlv_4"
            #     ],
            # ),
        ],name="digital_to_l1_aib_categoy_clean_master",
    )


def digital_to_l1_digital_mobile_web_agg_daily(**kwargs):
    return Pipeline(
        [
            node(
                l1_digital_mobile_web_category_agg_daily,
                [
                    "l0_digital_mobile_web_daily", "l1_digital_aib_categories_clean"
                ],
                [
                    "l1_digital_customer_web_category_agg_daily_catlv_1",
                ]
            ),
            # node(
            #     l1_digital_mobile_web_category_agg_timebrand,
            #     [
            #         "l0_digital_mobile_web_hourly", "l1_digital_aib_categories_clean"
            #     ],
            #     [
            #         "l1_digital_customer_web_category_agg_timebrand_catlv_1"
            #     ],
            # ),
        ],name="digital_to_l1_digital_mobile_web_agg_daily",
    )
