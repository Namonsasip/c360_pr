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

from customer360.utilities.config_parser import \
    l4_rolling_window, \
    node_from_config, \
    l4_rolling_ranked_window
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l4.to_l4_nodes import *


def streaming_l2_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            # # this will calculate sum per content_group
            node(
                streaming_two_output_function,
                    [
                        "int_l2_streaming_content_type_features_for_int_l4_streaming_content_type_features",
                        "params:int_l4_streaming_content_type_features",
                        "params:l4_streaming_fav_content_group_by_volume",
                        "params:l4_streaming_fav_content_group_by_duration",
                    ],
                 [
                     "l4_streaming_fav_content_group_by_volume", "l4_streaming_fav_content_group_by_duration"
                 ]
                 ),

            # # TV Channel features
            node(
                streaming_two_output_function,
                [
                    "int_l2_streaming_tv_channel_features_for_int_l4_streaming_tv_channel_features",
                    "params:int_l4_streaming_tv_channel_features",
                    "params:l4_streaming_fav_tv_channel_by_volume",
                    "params:l4_streaming_fav_tv_channel_by_duration",
                ],
                [
                    "l4_streaming_fav_tv_channel_by_volume", "l4_streaming_fav_tv_channel_by_duration"
                ]
            ),

            # # fav video service feature
            node(
                streaming_three_output_function,
                [
                    "int_l2_streaming_video_service_feature_for_int_l4_streaming_video_service_feature",
                    "params:int_l4_streaming_service_feature",
                    "params:l4_streaming_fav_service_by_download_feature",
                    "params:l4_streaming_2nd_fav_service_by_download_feature",
                    "params:l4_streaming_fav_service_by_visit_count_feature",
                ],
                [
                    "l4_streaming_fav_video_service_by_download_feature",
                    "l4_streaming_2nd_fav_video_service_by_download_feature",
                    "l4_streaming_fav_video_service_by_visit_count_feature"
                ]
            ),
            # fav music service feature
            node(
                streaming_three_output_function,
                [
                    "int_l2_streaming_music_service_feature_for_int_l4_streaming_music_service_feature",
                    "params:int_l4_streaming_service_feature",
                    "params:l4_streaming_fav_service_by_download_feature",
                    "params:l4_streaming_2nd_fav_service_by_download_feature",
                    "params:l4_streaming_fav_service_by_visit_count_feature",
                ],
                [
                    "l4_streaming_fav_music_service_by_download_feature",
                    "l4_streaming_2nd_fav_music_service_by_download_feature",
                    "l4_streaming_fav_music_service_by_visit_count_feature"
                ]
            ),

            # # fav esport service feature
            node(
                streaming_three_output_function,
                [
                    "int_l2_streaming_esport_service_feature_for_int_l4_streaming_music_service_feature",
                    "params:int_l4_streaming_service_feature",
                    "params:l4_streaming_fav_service_by_download_feature",
                    "params:l4_streaming_2nd_fav_service_by_download_feature",
                    "params:l4_streaming_fav_service_by_visit_count_feature",
                ],
                [
                    "l4_streaming_fav_esport_service_by_download_feature",
                    "l4_streaming_2nd_fav_esport_service_by_download_feature",
                    "l4_streaming_fav_esport_service_by_visit_count_feature"
                ]
            ),

            node(
                l4_rolling_window,
                [
                    "l2_streaming_visit_count_and_download_traffic_feature_for_l4_streaming_visit_count_and_download_traffic_feature",
                    "params:l4_streaming_visit_count_and_download_traffic_feature"],
                "l4_streaming_visit_count_and_download_traffic_feature"
            ),

            node(
                l4_rolling_window,
                ["int_l2_streaming_sum_per_day_for_l4_streaming_fav_youtube_video_streaming_day_of_week_feature",
                 "params:int_l4_streaming_download_traffic_per_day_of_week"],
                "int_l4_streaming_download_traffic_per_day_of_week"
            ),
            node(
                generate_l4_fav_streaming_day,
                ["int_l4_streaming_download_traffic_per_day_of_week",
                 "params:int_l4_streaming_ranked_of_day_per_rolling_week",
                 "params:streaming_app"],
                None
            ),
            node(
                node_from_config,
                ["l2_streaming_fav_tv_show_by_share_of_completed_episodes",
                 "params:l4_streaming_fav_tv_show_by_share_of_completed_episodes"],
                "l4_streaming_fav_tv_show_by_share_of_completed_episodes"
            )
        ], name="streaming_l2_to_l4_pipeline"
    )


def streaming_l2_to_l4_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                l4_rolling_window,
                ["l2_streaming_session_duration_feature_for_l4_streaming_session_duration_feature",
                 "params:l4_streaming_session_duration_feature"],
                "l4_streaming_session_duration_feature"
            ),
        ], name="streaming_l2_to_l4_session_duration_pipeline"
    )


def streaming_l1_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            # TV show features
            node(
                l4_rolling_window,
                ["int_l0_streaming_vimmi_table_for_l4_streaming_fav_tv_show_by_episode_watched",
                 "params:int_l4_streaming_tv_show_features_1"],
                "int_l4_streaming_tv_show_features_1"
            ),

            node(
                node_from_config,
                [
                    'int_l4_streaming_tv_show_features_1',
                    "params:int_l4_streaming_tv_show_features_2"
                ],
                "int_l4_streaming_tv_show_features_2"
            ),

            node(
                l4_rolling_ranked_window,
                ["int_l4_streaming_tv_show_features_2",
                 "params:l4_streaming_fav_tv_show_by_episode_watched"],
                "l4_streaming_fav_tv_show_by_episode_watched"
            ),

        ], name="streaming_l1_to_l4_pipeline"
    )
