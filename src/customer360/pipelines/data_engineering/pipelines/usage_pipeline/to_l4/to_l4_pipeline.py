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

from src.customer360.utilities.config_parser import l4_rolling_window


def usage_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_call_relation_sum_weekly",
            #      "params:l4_usage_call_relation_features"],
            #     "l4_usage_call_relation_features"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_call_relation_sum_ir_weekly",
            #      "params:l4_usage_call_relation_ir_features"],
            #     "l4_usage_call_relation_ir_features"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_data_prepaid_postpaid_weekly",
            #      "params:l4_usage_data_prepaid_postpaid_features"],
            #     "l4_usage_data_prepaid_postpaid_features"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_ru_a_vas_postpaid_prepaid_weekly",
            #      "params:l4_usage_ru_a_vas_postpaid_prepaid_features"],
            #     "l4_usage_ru_a_vas_postpaid_prepaid_features_stg"
            # ),
            # node(
            #     l4_rolling_window,
            #     ["l2_usage_ru_a_vas_postpaid_prepaid_weekly",
            #      "params:l4_usage_ru_a_vas_postpaid_prepaid_features"],
            #     "l4_usage_ru_a_vas_postpaid_prepaid_features"
            # )
            node(
                l4_rolling_window,
                ["l2_usage_postpaid_prepaid_weekly",
                 "params:l4_usage_postpaid_prepaid_weekly_features"],
                "l4_usage_postpaid_prepaid_weekly_features"

            )

        ], name="usage_to_l4_pipeline"
    )
