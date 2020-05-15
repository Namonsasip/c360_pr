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
from customer360.utilities.re_usable_functions import l1_massive_processing


def touchpoints_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l1_massive_processing,
                ["l0_usage_call_relation_sum_daily_for_l1_touchpoints_to_call_center_features",
                 "params:l1_touchpoints_to_call_center_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_to_call_center_features"],
                "l1_touchpoints_to_call_center_features"
            ),
            node(
                l1_massive_processing,
                ["l0_usage_call_relation_sum_daily_for_l1_touchpoints_from_call_center_features",
                 "params:l1_touchpoints_from_call_center_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_from_call_center_features"],
                "l1_touchpoints_from_call_center_features"
            ),
            node(
                l1_massive_processing,
                ["l0_complaints_ais_nim_work",
                 "params:l1_touchpoints_nim_work_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_nim_work_features"],
                "l1_touchpoints_nim_work_features"
            ),
            node(
                l1_massive_processing,
                ["l0_touchpoints_acc_ivr_log_daily",
                 "params:l1_touchpoints_ivr_features",
                 "l1_customer_profile_union_daily_feature_for_l1_touchpoints_ivr_features"],
                "l1_touchpoints_ivr_features"
            )
        ]
    )
