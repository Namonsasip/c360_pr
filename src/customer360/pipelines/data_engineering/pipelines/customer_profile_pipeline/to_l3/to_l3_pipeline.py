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

from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l3.to_l3_nodes import *


def customer_profile_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:int_l3_customer_profile_basic_features"],
                "int_l3_customer_profile_basic_features"
            ),
            node(
                union_daily_cust_profile,
                ["l0_customer_profile_profile_customer_profile_pre_current",
                 "l0_customer_profile_profile_customer_profile_post_current",
                 "l0_customer_profile_profile_customer_profile_post_non_mobile_current_non_mobile_current",
                 "params:union_customer_profile_column_to_extract"],
                "int_l3_customer_profile_union_features"
            ),
            node(
                merge_union_and_basic_features,
                ['int_l3_customer_profile_union_features',
                 'int_l3_customer_profile_basic_features'],
                "l3_customer_profile_features"
            )
        ]
    )


def customer_profile_billing_level_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ['l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly',
                 "params:l3_customer_profile_billing_level_number_of_mobile_devices"],
                "l3_customer_profile_billing_level_number_of_mobile_devices"
            ),
            node(
                node_from_config,
                ['l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly',
                 'params:l3_customer_profile_billing_level_number_of_sims'],
                "l3_customer_profile_billing_level_number_of_sims"
            ),
            node(
                node_from_config,
                ['l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly',
                 'params:l3_customer_profile_billing_level_number_of_active_contracts'],
                "l3_customer_profile_billing_level_number_of_active_contracts"
            ),
            node(
                node_from_config,
                ['l0_billing_statement_history_monthly',
                 "params:l3_customer_profile_billing_level_volume_of_active_contracts"],
                "l3_customer_profile_billing_level_volume_of_active_contracts"
            )
        ]
    )