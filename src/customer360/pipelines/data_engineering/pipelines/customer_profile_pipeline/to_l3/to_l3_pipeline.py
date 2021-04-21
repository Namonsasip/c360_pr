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

from customer360.utilities.config_parser import node_from_config
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l3.to_l3_nodes import *
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *

def customer_profile_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                 df_copy_for_l3_customer_profile_include_1mo_non_active,
                 "l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly_for_l3_1mo_non_active",
                 "int_l3_customer_profile_basic_features_1"
            ),

            # dev_lot4
            node(
                df_smp_for_l3_customer_profile_include_1mo_non_active,
                ["int_l3_customer_profile_basic_features_1",
                 "l0_customer_profile_profile_segment_movemet_profile_for_profile_drm_t_active_profile_customer_journey"],
                "int_l3_customer_profile_basic_features_2"
            ),

            node(
                df_profile_drm_t_serenade_master_post_for_l3_customer_profile_include_1mo_non_active,
                ["int_l3_customer_profile_basic_features_2",
                 "l0_customer_profile_drm_t_serenade_by_account_for_l3_profile_include_1mo_non_active",
                 "l0_customer_profile_lm_address_master_for_l3_profile_include_1mo_non_active"],
                "int_l3_customer_profile_basic_features_3"
            ),

            #lot6
            node(
                df_customer_profile_drm_t_newsub_prepaid_history_for_l3_profile_include_1mo_non_active,
                ["int_l3_customer_profile_basic_features_3",
                 "l0_customer_profile_drm_t_newsub_prepaid_history_for_l3_profile_include_1mo_non_active"],
                "l3_customer_profile_include_1mo_non_active_test_lot6"
             ),
            # node(
            #     node_from_config,
            #     ["int_l3_customer_profile_basic_features_4",
            #      "params:int_l3_customer_profile_basic_features"],
            #     "int_l3_customer_profile_basic_features_5"
            # ),
            # node(
            #     generate_modified_subscription_identifier,
            #     ["int_l3_customer_profile_basic_features_5"],
            #     "int_l3_customer_profile_basic_features_6"
            # ),
            # node(
            #     add_last_month_inactive_user,
            #     ["int_l3_customer_profile_basic_features_6"],
            #     "l3_customer_profile_include_1mo_non_active"
            # )
        ]
    )

def unioned_customer_profile_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                union_monthly_cust_profile,
                ["l1_customer_profile_union_daily_feature_for_l3_customer_profile_union_monthly_feature"],
                "l3_customer_profile_union_monthly_feature"
            ),
        ]
    )

def customer_profile_billing_level_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(df_copy_for_l3_customer_profile_billing_level_features,
                 "l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly_for_l3_customer_profile_billing_level_features",
                 "int_l3_customer_profile_billing_level_features"
                 ),
            node(
                node_from_config,
                ['int_l3_customer_profile_billing_level_features',
                 "params:l3_customer_profile_billing_level_features"],
                "l3_customer_profile_billing_level_features"
            ),
            node(df_copy_for_l3_customer_profile_billing_level_volume_of_active_contracts,
                 "l0_billing_statement_history_monthly_for_l3_customer_profile_billing_level_volume_of_active_contracts",
                 "int_l3_customer_profile_billing_level_volume_of_active_contracts"
                 ),
            node(
                node_from_config,
                ['int_l3_customer_profile_billing_level_volume_of_active_contracts',
                 "params:l3_customer_profile_billing_level_volume_of_active_contracts"],
                "l3_customer_profile_billing_level_volume_of_active_contracts"
            )
        ]
    )