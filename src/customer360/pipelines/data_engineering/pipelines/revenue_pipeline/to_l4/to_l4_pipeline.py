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
from customer360.utilities.config_parser import node_from_config

####
def revenue_to_l4_monthly_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                 "params:l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int"],
                "l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_stg"
            ),
            node(
                node_from_config,
                ["l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_stg",
                 "params:l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly"],
                "l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly"
            ),

            node(
                l4_rolling_window,
                ["l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                 "params:l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_int"],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_stg"
            ),
            node(
                node_from_config,
                ["l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_stg",
                 "params:l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"],
                "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly"
            ),

        ], name="revenue_to_l4_pipeline"
    )


def revenue_to_l4_daily_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l1_revenue_prepaid_pru_f_usage_multi_daily_for_l2_revenue_prepaid_pru_f_usage_multi",
                 "params:l4_revenue_prepaid_daily_features"],
                'l4_revenue_prepaid_daily_features'
            )

        ], name="revenue_to_l4_daily_pipeline"
    )


def revenue_to_l4_weekly_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l4_rolling_window,
                ["l2_revenue_prepaid_pru_f_usage_multi_weekly",
                 "params:l4_revenue_prepaid_pru_f_usage_multi_features"],
                "l4_revenue_prepaid_pru_f_usage_multi_features"
            ),

        ], name="revenue_to_l4_pipeline"
    )
