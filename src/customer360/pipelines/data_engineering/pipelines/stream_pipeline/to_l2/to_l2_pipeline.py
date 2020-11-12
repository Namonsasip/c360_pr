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

from customer360.utilities.config_parser import node_from_config, expansion
from customer360.utilities.re_usable_functions import l2_massive_processing, l2_massive_processing_with_expansion
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l2.to_l2_nodes import *


def streaming_to_l2_intermediate_pipeline(**kwargs):
    return Pipeline(
        [
            # TV Channel features
            node(streaming_to_l2_tv_channel_type_features,
                 [
                     "int_l1_streaming_tv_channel_features_for_int_l2_streaming_tv_channel_features",
                     "params:int_l2_streaming_tv_channel_features",
                     "params:l2_streaming_fav_tv_channel_by_volume",
                     "params:l2_streaming_fav_tv_channel_by_duration"],
                 [
                     "int_l2_streaming_tv_channel_features",
                     "l2_streaming_fav_tv_channel_by_volume",
                     "l2_streaming_fav_tv_channel_by_duration"
                 ]
                 ),
            # fav video service by download traffic/visit count
            # TV Channel features
            node(streaming_to_l2_esoprt_service_by_download,
                 [
                     "int_l1_streaming_esport_service_feature_for_int_l2_streaming_esport_service_feature",
                     "params:int_l2_streaming_esport_service_feature",
                     "params:l2_streaming_fav_esport_service_by_download_feature",
                     "params:l2_streaming_2nd_fav_esport_service_by_download_feature",
                     "params:l2_streaming_fav_esport_service_by_visit_count_feature"
                 ],
                 [
                     "int_l2_streaming_esport_service_feature",
                     "l2_streaming_fav_esport_service_by_download_feature",
                     "l2_streaming_2nd_fav_esport_service_by_download_feature",
                     "l2_streaming_fav_esport_service_by_visit_count_feature"
                 ]
                 ),
            # Favourite streaming day of week
            # get sum per day of week
            # rank of day per week
            node(streaming_streaming_ranked_of_day_per_week,
                 [
                     "l1_streaming_visit_count_and_download_traffic_feature_for_int_l2_streaming_sum_per_day",
                     "params:int_l2_streaming_sum_per_day",
                     "params:int_l2_streaming_ranked_of_day_per_week",
                     "params:streaming_app"

                  ], "int_l2_streaming_sum_per_day")
        ]
    )


def streaming_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # TV show features
            node(
                dac_for_streaming_to_l2_pipeline_from_l1,
                ["int_l0_streaming_vimmi_table_for_l2_streaming_fav_tv_show_by_episode_watched",
                 "params:l2_streaming_fav_tv_show_by_episode_watched_tbl"],
                "intermediate_int_l2_streaming_tv_show_features"
            ),
            node(
                node_from_config,
                ["intermediate_int_l2_streaming_tv_show_features",
                 "params:int_l2_streaming_tv_show_features"],
                "int_l2_streaming_tv_show_features"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_tv_show_features",
                 "params:l2_streaming_fav_tv_show_by_episode_watched"],
                "l2_streaming_fav_tv_show_by_episode_watched"
            ),
            #fav tv_show_by_share_of_completed_episodes
            node(
                streaming_to_l2_fav_tv_show_by_share_of_completed_episodes,
                ["int_l0_streaming_vimmi_table_for_l2_streaming_fav_tv_show_by_share_of_completed_episodes",
                 "l3_streaming_series_title_master",
                 "params:int_l2_streaming_share_of_completed_episodes_features",
                 "params:int_l2_streaming_share_of_completed_episodes_ratio_features",
                 "params:l2_streaming_fav_tv_show_by_share_of_completed_episodes"],
                "l2_streaming_fav_tv_show_by_share_of_completed_episodes"

            ),
            # number of visit and volume of download traffic
            node(
                dac_for_streaming_to_l2_pipeline_from_l1,
                [
                "l1_streaming_visit_count_and_download_traffic_feature_for_l2_streaming_visit_count_and_download_traffic_feature",
                "params:l2_streaming_visit_count_and_download_traffic_feature_tbl"],
                "intermediate_l2_streaming_visit_count_and_download_traffic_feature"
            ),
            node(
                l2_massive_processing_with_expansion,
                ["intermediate_l2_streaming_visit_count_and_download_traffic_feature",
                 "params:l2_streaming_visit_count_and_download_traffic_feature"],
                "l2_streaming_visit_count_and_download_traffic_feature"
            ),

        ], name="streaming_to_l2_pipeline"
    )


def streaming_to_l2_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                dac_for_streaming_to_l2_pipeline_from_l1,
                ["l1_streaming_session_duration_feature_for_l2_streaming_session_duration_feature",
                 "params:l2_streaming_session_duration_feature_tbl"],
                "intermediate_l2_streaming_session_duration_feature"
            ),
            node(
                l2_massive_processing,
                ["intermediate_l2_streaming_session_duration_feature",
                 "params:l2_streaming_session_duration_feature"],
                "l2_streaming_session_duration_feature"
            )
        ], name="streaming_to_l2_session_duration_pipeline"
    )
