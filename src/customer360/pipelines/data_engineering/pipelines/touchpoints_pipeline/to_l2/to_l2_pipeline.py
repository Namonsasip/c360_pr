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

from customer360.utilities.re_usable_functions import l2_massive_processing_with_expansion
from src.customer360.pipelines.data_engineering.nodes.touchpoints_nodes.to_l2.to_l2_nodes import *


def touchpoints_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                dac_for_touchpoints_to_l2_pipeline_from_l1,
                ["l1_touchpoints_to_call_center_features_for_l2_touchpoints_to_call_center_features",
                 "params:l2_touchpoints_to_call_center_features_tbl",
                 "params:exception_partition_list_for_l1_touchpoints_to_call_center_features_for_l2_touchpoints_to_call_center_features"],
                "intermediate_l2_touchpoints_to_call_center_features"
            ),
            node(
                l2_massive_processing_with_expansion,
                ["intermediate_l2_touchpoints_to_call_center_features",
                 "params:l2_touchpoints_to_call_center_features"],
                "l2_touchpoints_to_call_center_features"
            ),

            node(
                dac_for_touchpoints_to_l2_pipeline_from_l1,
                ["l1_touchpoints_from_call_center_features_for_l2_touchpoints_from_call_center_features",
                 "params:l2_touchpoints_from_call_center_features_tbl",
                 "params:exception_partition_list_for_l1_touchpoints_from_call_center_features_for_l2_touchpoints_from_call_center_features"],
                "intermediate_l2_touchpoints_from_call_center_features"
            ),
            node(
                l2_massive_processing_with_expansion,
                ["intermediate_l2_touchpoints_from_call_center_features",
                 "params:l2_touchpoints_from_call_center_features"],
                "l2_touchpoints_from_call_center_features"
            ),
            #
            # node(
            #     dac_for_touchpoints_to_l2_pipeline_from_l1,
            #     ["l1_touchpoints_nim_work_features_for_l2_touchpoints_nim_work_features",
            #      "params:l2_touchpoints_nim_work_features_tbl",
            #      "params:exception_partition_list_for_l1_touchpoints_nim_work_features_for_l2_touchpoints_nim_work_features"],
            #     "intermediate_l2_touchpoints_nim_work_features"
            # ),
            # node(
            #     l2_massive_processing_with_expansion,
            #     ["intermediate_l2_touchpoints_nim_work_features",
            #      "params:l2_touchpoints_nim_work_features"],
            #     "l2_touchpoints_nim_work_features"
            # ),
            #
            # node(
            #     dac_for_touchpoints_to_l2_pipeline_from_l1,
            #     ["l1_touchpoints_ivr_features_for_l2_touchpoints_ivr_features",
            #      "params:l2_touchpoints_ivr_features_tbl",
            #      "params:exception_partition_list_for_l1_touchpoints_ivr_features"],
            #     "intermediate_l2_touchpoints_ivr_features"
            # ),
            # node(
            #     l2_massive_processing_with_expansion,
            #     ["intermediate_l2_touchpoints_ivr_features",
            #      "params:l2_touchpoints_ivr_features"],
            #     "l2_touchpoints_ivr_features"
            # )
        ]
    )
