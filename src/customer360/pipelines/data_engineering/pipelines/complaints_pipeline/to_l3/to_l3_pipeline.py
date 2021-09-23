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
from customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l3.to_l3_nodes import *
from src.customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l3.to_l3_nodes import \
    run_for_complaints_to_l3_pipeline_from_l1

def complaints_to_l3_pipeline_survey(**kwargs):
    return Pipeline(
        [
            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_survey_after_call_for_l3_complaints_survey_after_call",
                 "params:l3_complaints_survey_after_call_tbl",
                 "params:l3_complaints_survey_after_call_scoring",
                 "params:exception_partition_list_for_monthly_l3_complaints_survey_after_call"
                 ],
                "l3_complaints_survey_after_call"
            ),

            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_survey_after_store_visit_for_l3_complaints_survey_after_store_visit",
                 "params:l3_complaints_survey_after_store_visit_tbl",
                 "params:l3_complaints_survey_after_store_visit",
                 "params:exception_partition_list_for_monthly_l3_complaints_survey_after_store_visit"
                 ],
                "l3_complaints_survey_after_store_visit"
            ),

            node(
                node_from_config,
                ["l1_complaints_survey_after_myais_for_l3_complaints_survey_after_myais",
                 "params:l3_complaints_survey_after_myais"],
                "l3_complaints_survey_after_myais"
            ),

            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_survey_after_chatbot_for_l3_complaints_survey_after_chatbot",
                 "params:l3_complaints_survey_after_chatbot_tbl",
                 "params:l3_complaints_survey_after_chatbot",
                 "params:exception_partition_list_for_monthly_l3_complaints_survey_after_chatbot"
                 ],
                "l3_complaints_survey_after_chatbot"
            ),
        ]
    )

def complaints_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_call_to_competitor_features_for_l3_complaints_call_to_competitor_features",
                 "params:l3_complaints_call_to_competitor_features_tbl",
                 "params:l3_complaints_call_to_competitor_features",
                 "params:exception_partition_list_for_monthly_l1_complaints_call_to_competitor_features"
                 ],
                "l3_complaints_call_to_competitor_features"
            ),

            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_nps_after_call_for_l3_complaints_nps_after_call",
                 "params:l3_complaints_nps_after_call_tbl",
                 "params:l3_complaints_nps_scoring",
                 "params:exception_partition_list_for_monthly_l1_complaints_nps_after_call"
                 ],
                "l3_complaints_nps_after_call"
            ),

            node(
                run_for_complaints_to_l3_pipeline_from_l1,
                ["l1_complaints_nps_after_store_visit_for_l3_complaints_nps_after_store_visit",
                 "params:l3_complaints_nps_after_store_visit_tbl",
                 "params:l3_complaints_nps_scoring",
                 "params:exception_partition_list_for_monthly_l1_complaints_nps_after_store_visit"
                 ],
                "l3_complaints_nps_after_store_visit"
            ),

            # node(
            #     run_for_complaints_to_l3_pipeline_from_l1_dtac,
            #     ["l1_complaints_traffic_to_dtac_web_resources_for_l3_complaints_traffic_to_dtac_web_resources",
            #      "params:l3_complaints_traffic_to_dtac_web_resources_tbl",
            #      "params:l3_complaints_traffic_to_dtac_web_resources",
            #      "params:exception_partition_list_for_monthly_l3_complaints_traffic_to_dtac_web_resources"
            #      ],
            #     "l3_complaints_traffic_to_dtac_web_resources"
            # ),

        ]
    )
