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

from customer360.pipelines.data_engineering.nodes.campaign_nodes.to_l1 import cam_post_channel_with_highest_conversion


def campaign_to_l1_pipeline(**kwargs):
    return Pipeline(
        # [
        #     node(
        #         cam_post_channel_with_highest_conversion,
        #         ['l0_campaign_tracking_contact_list_post',
        #          'l0_campaign_tracking_contact_list_pre',
        #          'l1_customer_profile_union_daily_feature_for_l1_campaign_post_pre_daily',
        #          'params:l1_campaign_post_pre_daily',
        #          'params:l1_campaign_top_channel_daily'],
        #         ['l1_campaign_post_pre_daily', 'l1_campaign_top_channel_daily']
        #     )
        #
        # ], name="campaign_to_l1_pipeline"
        [
            node(
                cam_post_channel_with_highest_conversion,
                ['l0_campaign_tracking_contact_list_post',
                 'l0_campaign_tracking_contact_list_pre',
                 'l0_campaign_tracking_contact_list_fbb',
                 'params:l1_campaign_post_pre_daily',
                 'params:l1_campaign_top_channel_daily',
                 'params:l1_campaign_detail_daily'
                 ],
                ['l1_campaign_post_pre_daily', 'l1_campaign_top_channel_daily', 'l1_campaign_detail_daily']
            )

        ], name="campaign_to_l1_pipeline"
    )

def campaign_to_l1_pipeline_hdfs(**kwargs):
    return Pipeline(
        [
            node(
                cam_post_channel_with_highest_conversion,
                ['l0_campaign_tracking_contact_list_post_hdfs',
                 'l0_campaign_tracking_contact_list_pre_hdfs',
                 'l0_campaign_tracking_contact_list_fbb_hdfs',
                 'params:l1_campaign_post_pre_daily',
                 'params:l1_campaign_top_channel_daily',
                 'params:l1_campaign_detail_daily'
                 ],
                ['l1_campaign_post_pre_daily', 'l1_campaign_top_channel_daily', 'l1_campaign_detail_daily']
            )

        ], name="campaign_to_l1_pipeline_hdfs"
    )
