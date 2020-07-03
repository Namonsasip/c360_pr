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
from customer360.utilities.re_usable_functions import l3_massive_processing
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l3.to_l3_nodes import *


def streaming_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            # # Content Type Features
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_content_type_features_for_l3_streaming_fav_content_group_by_volume",
            #      "params:l3_streaming_fav_content_group_by_volume_tbl"],
            #     "intermediate_int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_volume"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_volume",
            #      "params:int_l3_streaming_content_type_features"],
            #     "int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_volume"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_volume",
            #      "params:l3_streaming_fav_content_group_by_volume"],
            #     "l3_streaming_fav_content_group_by_volume"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_content_type_features_for_l3_streaming_fav_content_group_by_duration",
            #      "params:l3_streaming_fav_content_group_by_duration_tbl"],
            #     "intermediate_int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_duration",
            #      "params:int_l3_streaming_content_type_features"],
            #     "int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_content_type_features_for_l3_streaming_fav_content_group_by_duration",
            #      "params:l3_streaming_fav_content_group_by_duration"],
            #     "l3_streaming_fav_content_group_by_duration"
            # ),
            node(
                streaming_to_l3_content_type_features,
                ["int_l1_streaming_content_type_features_for_l3_streaming_fav_content_group",
                 "params:int_l3_streaming_content_type_features",
                 "params:l3_streaming_fav_content_group_by_volume",
                 "params:l3_streaming_fav_content_group_by_duration",
                 ],
                ["l3_streaming_fav_content_group_by_volume", "l3_streaming_fav_content_group_by_duration"]
            ),

            # # TV Channel features
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_volume",
            #      "params:l3_streaming_fav_tv_channel_by_volume_tbl"],
            #     "intermediate_int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_volume"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_volume",
            #      "params:int_l3_streaming_tv_channel_features"],
            #     "int_l3_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_volume"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_volume",
            #      "params:l3_streaming_fav_tv_channel_by_volume"],
            #     "l3_streaming_fav_tv_channel_by_volume"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_duration",
            #      "params:l3_streaming_fav_tv_channel_by_duration_tbl"],
            #     "intermediate_int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_duration",
            #      "params:int_l3_streaming_tv_channel_features"],
            #     "int_l3_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel_by_duration",
            #      "params:l3_streaming_fav_tv_channel_by_duration"],
            #     "l3_streaming_fav_tv_channel_by_duration"
            # ),
            node(
                streaming_to_l3_tv_channel_type_features,
                ["int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel",
                 "params:int_l3_streaming_tv_channel_features",
                 "params:l3_streaming_fav_tv_channel_by_volume",
                 "params:l3_streaming_fav_tv_channel_by_duration",
                 ],
                ["l3_streaming_fav_tv_channel_by_volume", "l3_streaming_fav_tv_channel_by_duration"]
            ),

            # # TV show features
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_episode_watched",
            #      "params:l3_streaming_fav_tv_show_by_episode_watched_tbl"],
            #     "intermediate_int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_episode_watched"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_episode_watched",
            #      "params:int_l3_streaming_tv_show_features"],
            #     "int_l3_streaming_tv_show_features"
            # ),
            # node(
            #     dac_for_l3_streaming_fav_tv_show_by_episode_watched,
            #     ["int_l3_streaming_tv_show_features",
            #      "l3_customer_profile_include_1mo_non_active_for_l3_streaming_fav_tv_show_by_episode_watched"],
            #     ["intermediate_int_l3_streaming_tv_show_features",
            #      "intermediate_l3_customer_profile_include_1mo_non_active_for_l3_streaming_fav_tv_show_by_episode_watched"]
            #
            # ),
            # node(
            #     l3_massive_processing,
            #     ["intermediate_int_l3_streaming_tv_show_features",
            #      "params:l3_streaming_fav_tv_show_by_episode_watched",
            #      "intermediate_l3_customer_profile_include_1mo_non_active_for_l3_streaming_fav_tv_show_by_episode_watched"],
            #     "l3_streaming_fav_tv_show_by_episode_watched"
            # ),
            node(streaming_streaming_fav_tv_show_by_episode_watched_features,
                 [
                     "int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_episode_watched",
                     "params:int_l3_streaming_tv_show_features",
                     "params:l3_streaming_fav_tv_show_by_episode_watched",
                  ],
                 "l3_streaming_fav_tv_show_by_episode_watched"
                 ),

            # # fav video service by download traffic/visit count
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_download_feature",
            #      "params:l3_streaming_fav_video_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_download_feature",
            #      "params:l3_streaming_fav_service_by_download_feature"],
            #     "l3_streaming_fav_video_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_video_service_feature_for_l3_streaming_2nd_fav_video_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_video_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_2nd_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_2nd_fav_video_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_video_service_feature_for_l3_streaming_2nd_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_video_service_feature_for_l3_streaming_2nd_fav_video_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_service_by_download_feature"],
            #     "l3_streaming_2nd_fav_video_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_video_service_by_visit_count_feature_tbl"],
            #     "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_visit_count_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_video_service_feature_for_l3_streaming_fav_video_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_service_by_visit_count_feature"],
            #     "l3_streaming_fav_video_service_by_visit_count_feature"
            # ),
            node(streaming_fav_service_download_traffic_visit_count,
                 [
                     "int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_feature",
                     "params:int_l3_streaming_service_feature",
                     "params:l3_streaming_fav_video_service_by_download_feature",
                     "params:l3_streaming_2nd_fav_video_service_by_download_feature",
                     "params:l3_streaming_fav_video_service_by_visit_count_feature"
                  ],
                 [
                     "l3_streaming_fav_video_service_by_download_feature",
                     "l3_streaming_2nd_fav_video_service_by_download_feature",
                     "l3_streaming_fav_video_service_by_visit_count_feature"
                  ]
                 ),

            # # fav music service by download traffic/visit count
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_download_feature",
            #      "params:l3_streaming_fav_music_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_download_feature",
            #      "params:l3_streaming_fav_service_by_download_feature"],
            #     "l3_streaming_fav_music_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_music_service_feature_for_l3_streaming_2nd_fav_music_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_music_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_2nd_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_2nd_fav_music_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_music_service_feature_for_l3_streaming_2nd_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_music_service_feature_for_l3_streaming_2nd_fav_music_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_service_by_download_feature"],
            #     "l3_streaming_2nd_fav_music_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_music_service_by_visit_count_feature_tbl"],
            #     "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_visit_count_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_music_service_feature_for_l3_streaming_fav_music_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_service_by_visit_count_feature"],
            #     "l3_streaming_fav_music_service_by_visit_count_feature"
            # ),
            node(streaming_fav_service_download_traffic_visit_count,
                 [
                     "int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_feature",
                     "params:int_l3_streaming_service_feature",
                     "params:l3_streaming_fav_music_service_by_download_feature",
                     "params:l3_streaming_2nd_fav_music_service_by_download_feature",
                     "params:l3_streaming_fav_music_service_by_visit_count_feature"
                 ],
                 [
                     "l3_streaming_fav_music_service_by_download_feature",
                     "l3_streaming_2nd_fav_music_service_by_download_feature",
                     "l3_streaming_fav_music_service_by_visit_count_feature"
                 ]
                 ),

            # # fav esport service by download traffic/visit count
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_download_feature",
            #      "params:l3_streaming_fav_esport_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_download_feature",
            #      "params:l3_streaming_fav_service_by_download_feature"],
            #     "l3_streaming_fav_esport_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_esport_service_feature_for_l3_streaming_2nd_fav_esport_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_esport_service_by_download_feature_tbl"],
            #     "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_2nd_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_2nd_fav_esport_service_by_download_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_esport_service_feature_for_l3_streaming_2nd_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_esport_service_feature_for_l3_streaming_2nd_fav_esport_service_by_download_feature",
            #      "params:l3_streaming_2nd_fav_service_by_download_feature"],
            #     "l3_streaming_2nd_fav_esport_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l3_pipeline_from_l1,
            #     ["int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_esport_service_by_visit_count_feature_tbl"],
            #     "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     [
            #         "intermediate_int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_visit_count_feature",
            #         "params:int_l3_streaming_service_feature"],
            #     "int_l3_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_visit_count_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l3_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_by_visit_count_feature",
            #      "params:l3_streaming_fav_service_by_visit_count_feature"],
            #     "l3_streaming_fav_esport_service_by_visit_count_feature"
            # ),
            node(streaming_fav_service_download_traffic_visit_count,
                 [
                     "int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_feature",
                     "params:int_l3_streaming_service_feature",
                     "params:l3_streaming_fav_esport_service_by_download_feature",
                     "params:l3_streaming_2nd_fav_esport_service_by_download_feature",
                     "params:l3_streaming_fav_esport_service_by_visit_count_feature"
                 ],
                 [
                     "l3_streaming_fav_esport_service_by_download_feature",
                     "l3_streaming_2nd_fav_esport_service_by_download_feature",
                     "l3_streaming_fav_esport_service_by_visit_count_feature"
                 ]
                 ),

            # number of visit and volume of download traffic
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                [
                    "l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_visit_count_and_download_traffic_feature",
                    "params:l3_streaming_visit_count_and_download_traffic_feature_tbl"
                ],
                "intermediate_l3_streaming_visit_count_and_download_traffic_feature"
            ),
            node(
                expansion,
                ["intermediate_l3_streaming_visit_count_and_download_traffic_feature",
                 "params:l3_streaming_visit_count_and_download_traffic_feature"],
                "l3_streaming_visit_count_and_download_traffic_feature"
            ),

            # Favourite streaming day of week
            # get sum per day of week
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                [
                    "l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature",
                    "params:l3_streaming_fav_youtube_video_streaming_day_of_week_feature_tbl"],
                "intermediate_l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature"
            ),
            node(
                expansion,
                [
                    "intermediate_l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature",
                    "params:int_l3_streaming_sum_per_day"],
                "int_l3_streaming_sum_per_day"
            ),
            # rank of day per week
            node(
                node_from_config,
                ["int_l3_streaming_sum_per_day",
                 "params:int_l3_streaming_ranked_of_day_per_month"],
                "int_l3_streaming_ranked_of_day_per_month"
            ),
            # generate all the tables inside
            node(
                generate_l3_fav_streaming_day,
                ["int_l3_streaming_ranked_of_day_per_month",
                 "params:streaming_app"],
                None
            ),
        ], name="streaming_to_l3_pipeline"
    )


def streaming_to_l3_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                ["l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature",
                 "params:l3_streaming_session_duration_feature_tbl"],
                "intermediate_l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature"
            ),
            node(
                node_from_config,
                ["intermediate_l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature",
                 "params:l3_streaming_session_duration_feature"],
                "l3_streaming_session_duration_feature"
            )
        ], name="streaming_to_l3_session_duration_pipeline"
    )