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

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import l1_massive_processing, add_start_of_week_and_month
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l1.to_l1_nodes import *


def streaming_to_l1_intermediate_pipeline(**kwargs):
    return Pipeline(
        [
            # MOVED TO SECTION 1
            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_content_type_features",
            #      "l1_customer_profile_union_daily_feature_for_int_l1_streaming_content_type_features",
            #      "params:int_l1_streaming_content_type_features_tbl"],
            #     ["int_l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_content_type_features",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_content_type_features"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_content_type_features",
            #      "params:int_l1_streaming_content_type_features",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_content_type_features"],
            #     "int_l1_streaming_content_type_features"
            # ),
            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_tv_channel_features",
            #      "l1_customer_profile_union_daily_feature_for_int_l1_streaming_tv_channel_features",
            #      "params:int_l1_streaming_tv_channel_features_tbl"],
            #     ["int_l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_tv_channel_features",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_tv_channel_features"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l1_streaming_tv_channel_features",
            #      "params:int_l1_streaming_tv_channel_features",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_tv_channel_features"],
            #     "int_l1_streaming_tv_channel_features"
            # ),
            # The above four nodes will be clubbed into  one node for optimization as well from down second pipeline
            # i have added 8 nodes and merged into this one

            # SECTION 1 BELOW
            node(stream_process_ru_a_onair_vimmi,
                 ["l0_streaming_ru_a_onair_vimmi_usage_daily_for_multiple_outputs",
                  "l1_customer_profile_union_daily_feature_for_l1_streaming_fav_tv_show_by_episode_watched",
                  # Content Type Features
                  "params:int_l1_streaming_content_type_features",
                  "params:l1_streaming_fav_content_group_by_volume",
                  "params:l1_streaming_fav_content_group_by_duration",

                  # TV Channel features
                  "params:int_l1_streaming_tv_channel_features",
                  "params:l1_streaming_fav_tv_channel_by_volume",
                  "params:l1_streaming_fav_tv_channel_by_duration",

                  # Favorite Episode
                  "params:int_l1_streaming_tv_show_features",
                  "params:l1_streaming_fav_tv_show_by_episode_watched",

                  ],
                 [
                  # Content Type Features
                  "int_l1_streaming_content_type_features", "int_l1_streaming_tv_channel_features",
                  "l1_streaming_fav_content_group_by_volume",
                  # TV Channel features
                  "l1_streaming_fav_content_group_by_duration",
                  "l1_streaming_fav_tv_channel_by_volume_df", "l1_streaming_fav_tv_channel_by_duration_df",
                  # TV Show features
                  "int_l0_streaming_vimmi_table",
                  # Favorite Episode
                  "l1_streaming_fav_tv_show_by_episode_watched"
                  ]
                 ),

            node(stream_process_soc_mobile_data,
                 ["l0_streaming_soc_mobile_app_daily_for_multiple_outputs",
                  "l1_customer_profile_union_daily_feature_for_l1_streaming_visit_count_and_download_traffic_feature",
                  # # fav video service by download traffic
                  "params:int_l1_streaming_video_service_feature",
                  "params:l1_streaming_fav_video_service_by_download_feature",
                  "params:l1_streaming_2nd_fav_video_service_by_download_feature",
                  # fav music service by download traffic
                  "params:int_l1_streaming_music_service_feature",
                  "params:l1_streaming_fav_music_service_by_download_feature",
                  "params:l1_streaming_2nd_fav_music_service_by_download_feature",

                  # # # fav esport service by download traffic
                  "params:int_l1_streaming_esport_service_feature",
                  "params:l1_streaming_fav_esport_service_by_download_feature",
                  "params:l1_streaming_2nd_fav_esport_service_by_download_feature",

                  # # number of visit and volume of download traffic
                  "params:l1_streaming_visit_count_and_download_traffic_feature"
                  ],
                 ["int_l1_streaming_video_service_feature", "l1_streaming_fav_video_service_by_download_feature",
                  "l1_streaming_2nd_fav_video_service_by_download_feature",
                  "int_l1_streaming_music_service_feature", "l1_streaming_fav_music_service_by_download_feature",
                  "l1_streaming_2nd_fav_music_service_by_download_feature",
                  "int_l1_streaming_esport_service_feature", "l1_streaming_fav_esport_service_by_download_feature",
                  "l1_streaming_2nd_fav_esport_service_by_download_feature",
                  "l1_streaming_visit_count_and_download_traffic_feature"]
                 ),

            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_video_service_feature",
            #      "l1_customer_profile_union_daily_feature_for_int_l1_streaming_video_service_feature",
            #      "params:int_l1_streaming_video_service_feature_tbl"],
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_video_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_video_service_feature"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_video_service_feature",
            #      "params:int_l1_streaming_video_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_video_service_feature"],
            #     "int_l1_streaming_video_service_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_music_service_feature",
            #      "l1_customer_profile_union_daily_feature_for_int_l1_streaming_music_service_feature",
            #      "params:int_l1_streaming_music_service_feature_tbl"],
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_music_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_music_service_feature"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_music_service_feature",
            #      "params:int_l1_streaming_music_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_music_service_feature"],
            #     "int_l1_streaming_music_service_feature"
            # ),

            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_esport_service_feature",
            #      "l1_customer_profile_union_daily_feature_for_int_l1_streaming_esport_service_feature",
            #      "params:int_l1_streaming_esport_service_feature_tbl"],
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_esport_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_esport_service_feature"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_soc_mobile_app_daily_for_int_l1_streaming_esport_service_feature",
            #      "params:int_l1_streaming_esport_service_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_int_l1_streaming_esport_service_feature"],
            #     "int_l1_streaming_esport_service_feature"
            # ),

        ]
    )


def streaming_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # MOVED TO SECTION 1
            # Content Type Features
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_content_type_features_for_l1_streaming_fav_content_group_by_volume",
            #      "params:l1_streaming_fav_content_group_by_volume_tbl"],
            #     "int_l1_streaming_fav_content_group_by_volume"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_content_group_by_volume",
            #      "params:l1_streaming_fav_content_group_by_volume"],
            #     "l1_streaming_fav_content_group_by_volume"
            # ),
            #
            #
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_content_type_features_for_l1_streaming_fav_content_group_by_duration",
            #      "params:l1_streaming_fav_content_group_by_duration_tbl"],
            #     "int_l1_streaming_fav_content_group_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_content_group_by_duration",
            #      "params:l1_streaming_fav_content_group_by_duration"],
            #     "l1_streaming_fav_content_group_by_duration"
            # ),
            # The above four nodes is also merged with the above code in section one

            # # TV Channel features
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_tv_channel_features_for_l1_streaming_fav_tv_channel_by_volume",
            #      "params:l1_streaming_fav_tv_channel_by_volume_tbl"],
            #     "int_l1_streaming_fav_tv_channel_by_volume"
            # ),
            #
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_tv_channel_by_volume",
            #      "params:l1_streaming_fav_tv_channel_by_volume"],
            #     "l1_streaming_fav_tv_channel_by_volume"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_tv_channel_features_for_l1_streaming_fav_tv_channel_by_duration",
            #      "params:l1_streaming_fav_tv_channel_by_duration_tbl"],
            #     "int_l1_streaming_fav_tv_channel_by_duration"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_tv_channel_by_duration",
            #      "params:l1_streaming_fav_tv_channel_by_duration"],
            #     "l1_streaming_fav_tv_channel_by_duration"
            # ),

            # # TV Show features
            # node(
            #     dac_for_streaming_to_l1_pipeline_from_l0,
            #     ["l0_streaming_ru_a_onair_vimmi_usage_daily_for_int_l0_streaming_vimmi_table",
            #      "params:int_l0_streaming_vimmi_table_tbl"],
            #     "intermediate_int_l0_streaming_vimmi_table"
            # ),
            # node(
            #     add_start_of_week_and_month,
            #     ["intermediate_int_l0_streaming_vimmi_table"],
            #     "int_l0_streaming_vimmi_table"
            # ),

            # node(
            #     dac_for_streaming_to_l1_pipeline_from_l0,
            #     ["l0_streaming_ru_a_onair_vimmi_usage_daily_for_l1_streaming_fav_tv_show_by_episode_watched",
            #      "params:l1_streaming_fav_tv_show_by_episode_watched_tbl"],
            #     "intermediate_int_l1_streaming_tv_show_features"
            # ),
            # node(
            #     node_from_config,
            #     ["intermediate_int_l1_streaming_tv_show_features",
            #      "params:int_l1_streaming_tv_show_features"],
            #     "int_l1_streaming_tv_show_features"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_tv_show_features",
            #      "params:l1_streaming_fav_tv_show_by_episode_watched"],
            #     "l1_streaming_fav_tv_show_by_episode_watched"
            # ),

            # # fav video service by download traffic
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_video_service_feature_for_l1_streaming_fav_video_service_by_download_feature",
            #      "params:l1_streaming_fav_video_service_by_download_feature_tbl"],
            #     "int_l1_streaming_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_video_service_by_download_feature",
            #      "params:l1_streaming_fav_video_service_by_download_feature"],
            #     "l1_streaming_fav_video_service_by_download_feature"
            # ),
            #
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_video_service_feature_for_l1_streaming_2nd_fav_video_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_video_service_by_download_feature_tbl"],
            #     "int_l1_streaming_2nd_fav_video_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_2nd_fav_video_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_video_service_by_download_feature"],
            #     "l1_streaming_2nd_fav_video_service_by_download_feature"
            # ),

            # # # fav music service by download traffic
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_music_service_feature_for_l1_streaming_fav_music_service_by_download_feature",
            #      "params:l1_streaming_fav_music_service_by_download_feature_tbl"],
            #     "int_l1_streaming_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_music_service_by_download_feature",
            #      "params:l1_streaming_fav_music_service_by_download_feature"],
            #     "l1_streaming_fav_music_service_by_download_feature"
            # ),
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_music_service_feature_for_streaming_2nd_fav_music_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_music_service_by_download_feature_tbl"],
            #     "int_l1_streaming_2nd_fav_music_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_2nd_fav_music_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_music_service_by_download_feature"],
            #     "l1_streaming_2nd_fav_music_service_by_download_feature"
            # ),

            # # # fav esport service by download traffic
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_esport_service_feature_for_l1_streaming_fav_esport_service_by_download_feature",
            #      "params:l1_streaming_fav_esport_service_by_download_feature_tbl"],
            #     "int_l1_streaming_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_fav_esport_service_by_download_feature",
            #      "params:l1_streaming_fav_esport_service_by_download_feature"],
            #     "l1_streaming_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     dac_for_streaming_to_l1_pipeline,
            #     ["int_l1_streaming_esport_service_feature_for_l1_streaming_2nd_fav_esport_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_esport_service_by_download_feature_tbl"],
            #     "int_l1_streaming_2nd_fav_esport_service_by_download_feature"
            # ),
            # node(
            #     node_from_config,
            #     ["int_l1_streaming_2nd_fav_esport_service_by_download_feature",
            #      "params:l1_streaming_2nd_fav_esport_service_by_download_feature"],
            #     "l1_streaming_2nd_fav_esport_service_by_download_feature"
            # ),

            # # number of visit and volume of download traffic
            # node(
            #     dac_for_streaming_to_l1_intermediate_pipeline,
            #     ["l0_streaming_soc_mobile_app_daily_for_l1_streaming_visit_count_and_download_traffic_feature",
            #      "l1_customer_profile_union_daily_feature_for_l1_streaming_visit_count_and_download_traffic_feature",
            #      "params:l1_streaming_visit_count_and_download_traffic_feature_tbl"],
            #     ["int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_visit_count_and_download_traffic_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_streaming_visit_count_and_download_traffic_feature"]
            # ),
            # node(
            #     l1_massive_processing,
            #     ["int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_visit_count_and_download_traffic_feature",
            #      "params:l1_streaming_visit_count_and_download_traffic_feature",
            #      "int_l1_customer_profile_union_daily_feature_for_l1_streaming_visit_count_and_download_traffic_feature"],
            #     "l1_streaming_visit_count_and_download_traffic_feature"
            # ),

        ], name="streaming_to_l1_pipeline"
    )


def streaming_to_l1_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                dac_for_streaming_to_l1_intermediate_pipeline,
                ["l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                 "l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature",
                 "params:l1_streaming_session_duration_feature_tbl"],
                ["int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                 "int_l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature"]
            ),
            node(
                application_duration,
                ["int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                 "l0_mobile_app_master_for_l1_streaming_session_duration_feature"],
                "int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature_application"
            ),
            node(
                l1_massive_processing,
                ["int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature_application",
                 "params:l1_streaming_session_duration_feature",
                 "int_l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature"],
                "l1_streaming_session_duration_feature"
            )
        ], name="streaming_to_l1_session_duration_pipeline"
    )
