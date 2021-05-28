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

from customer360.pipelines.data_engineering.nodes.digital_nodes.to_l3 import build_digital_l3_monthly_features


def digital_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_digital_l3_monthly_features,
                ["l0_digital_cxenxse_user_profile_monthly",
                 "l3_customer_profile_union_monthly_feature_for_l3_digital_cxenxse_user_profile_monthly",
                 "params:l3_digital_cxenxse_user_profile_monthly",
                 "params:exception_partition_list_for_l0_digital_cxenxse_user_profile_monthly"],
                "l3_digital_cxenxse_user_profile_monthly"
            ),
        ], name="digital_to_l3_pipeline"
    )

def digital_to_l3_customer_relay_agg_monthly(**kwargs):
    return Pipeline(
        [
            node(
                func=digital_customer_relay_pageview_agg_monthly,
                inputs=["l0_digital_relay_pageviews",
                        "params:l3_digital_relay_engagement_pageview_count_visit_by_cid_monthly"],
                outputs="l3_digital_customer_relay_pageview_agg_monthly",
                tags=["digital_customer_relay_pageview_agg_monthly"],
            )
        ]
    )


