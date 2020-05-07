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

from src.customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l2.to_l2_nodes import \
    dac_for_complaints_to_l2_pipeline


def complaints_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            #node(
            #    node_from_config,
            #    ["l1_complaints_call_to_competitor_features_for_l2_complaints_call_to_competitor_features",
            #     "params:l2_complaints_call_to_competitor_features"],
            #    "l2_complaints_call_to_competitor_features"
            #),

            node(
                dac_for_complaints_to_l2_pipeline,
                ["l1_complaints_call_to_competitor_features_for_l2_complaints_call_to_competitor_features",
                 "params:l2_complaints_call_to_competitor_features_tbl"],
                "int_l1_complaints_call_to_competitor_features_for_l2_complaints_call_to_competitor_features"
            ),
            node(
                node_from_config,
                ["int_l1_complaints_call_to_competitor_features_for_l2_complaints_call_to_competitor_features",
                 "params:l2_complaints_call_to_competitor_features"],
                "l2_complaints_call_to_competitor_features"
            ),

            #node(
            #    node_from_config,
            #    ["l1_complaints_nps_after_call_for_l2_complaints_nps_after_call",
            #     "params:l2_complaints_nps_scoring"],
            #    "l2_complaints_nps_after_call"
            #),
            node(
                dac_for_complaints_to_l2_pipeline,
                ["l1_complaints_nps_after_call_for_l2_complaints_nps_after_call",
                 "params:l2_complaints_nps_scoring_tbl"],
                "int_l1_complaints_nps_after_call_for_l2_complaints_nps_after_call"
            ),
            node(
                node_from_config,
                ["int_l1_complaints_nps_after_call_for_l2_complaints_nps_after_call",
                 "params:l2_complaints_nps_scoring"],
                "l2_complaints_nps_after_call"
            ),

            #node(
            #    node_from_config,
            #    ["l1_complaints_nps_after_chatbot_for_l2_complaints_nps_after_chatbot",
            #     "params:l2_complaints_nps_scoring"],
            #    "l2_complaints_nps_after_chatbot"
            #),
            node(
                dac_for_complaints_to_l2_pipeline,
                ["l1_complaints_nps_after_chatbot_for_l2_complaints_nps_after_chatbot",
                 "params:l2_complaints_nps_scoring_tbl"],
                "int_l1_complaints_nps_after_chatbot_for_l2_complaints_nps_after_chatbot"
            ),
            node(
                node_from_config,
                ["l1_complaints_nps_after_chatbot_for_l2_complaints_nps_after_chatbot",
                 "params:l2_complaints_nps_scoring"],
                "l2_complaints_nps_after_chatbot"
            ),

            #node(
            #    node_from_config,
            #    ["l1_complaints_nps_after_store_visit_for_l2_complaints_nps_after_store_visit",
            #     "params:l2_complaints_nps_scoring"],
            #    "l2_complaints_nps_after_store_visit"
            #)
            node(
                dac_for_complaints_to_l2_pipeline,
                ["l1_complaints_nps_after_store_visit_for_l2_complaints_nps_after_store_visit",
                 "params:l2_complaints_nps_scoring_tbl"],
                "int_l1_complaints_nps_after_store_visit_for_l2_complaints_nps_after_store_visit"
            ),
            node(
                node_from_config,
                ["l1_complaints_nps_after_store_visit_for_l2_complaints_nps_after_store_visit",
                 "params:l2_complaints_nps_scoring"],
                "l2_complaints_nps_after_store_visit"
            )

        ]
    )
