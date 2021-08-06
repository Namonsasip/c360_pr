from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *

def billing_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # Weekly top up count and top up volume pre-paid
            node(
                billing_topup_count_and_volume_node_weekly,
                ["l1_billing_and_payments_daily_topup_and_volume_for_l2_billing_and_payments_weekly_topup_and_volume",
                 "params:l2_billing_and_payment_feature_top_up_and_count_weekly",
                 "params:exception_partition_list_for_l1_billing_and_payments_daily_topup_and_volume_for_l2_billing_and_payments_weekly_topup_and_volume"],
                "l2_billing_and_payments_weekly_topup_and_volume"
            ),

            # Weekly Time difference between top ups pre-paid
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l2_billing_and_payments_weekly_topup_time_diff",
                 "params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate"],
                "l2_billing_and_payments_weekly_topup_diff_time_intermediate"
            ),
            node(
                billing_time_diff_between_topups_weekly,
                ["l1_customer_profile_union_daily_feature_for_l2_billing_and_payments_weekly_topup_time_diff",
                 "l2_billing_and_payments_weekly_topup_diff_time_intermediate",
                 "params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly",
                 "params:exception_partition_list_for_l1_customer_profile_union_daily_feature_for_l2_billing_and_payments_weekly_topup_time_diff",
                 "params:exception_partition_list_for_l2_billing_and_payments_weekly_topup_diff_time_intermediate"
                 ],
                "l2_billing_and_payments_weekly_topup_time_diff"
            ),

            # Weekly arpu of roaming post-paid
            node(
                billing_arpu_roaming_weekly,
                ["l1_billing_and_payments_daily_rpu_roaming_for_l2_billing_and_payments_weekly_rpu_roaming",
                 "params:l2_billing_and_payment_feature_rpu_roaming_weekly",
                 "params:exception_partition_list_for_l1_billing_and_payments_daily_rpu_roaming_for_l2_billing_and_payments_weekly_rpu_roaming"
                 ],
                "l2_billing_and_payments_weekly_rpu_roaming"
            ),

            # Weekly balance before top up pre-paid
            node(
                billing_before_topup_balance_weekly,
                ["l1_billing_and_payments_daily_before_top_up_balance_for_l2_billing_and_payments_weekly_before_top_up_balance",
                 "params:l2_billing_and_payment_before_top_up_balance_weekly",
                 "params:exception_partition_list_for_l1_billing_and_payments_daily_before_top_up_balance_for_l2_billing_and_payments_weekly_before_top_up_balance"
                 ],
                "l2_billing_and_payments_weekly_before_top_up_balance"
            ),

            # Weekly top up channels pre-paid
            node(
                billing_top_up_channels_weekly,
                ["l1_billing_and_payments_daily_top_up_channels_for_l2_billing_and_payments_weekly_top_up_channels",
                 "params:l2_billing_and_payment_top_up_channels_weekly",
                 "params:exception_partition_list_for_l1_billing_and_payments_daily_top_up_channels_for_l2_billing_and_payments_weekly_top_up_channels"
                 ],
                "l2_billing_and_payments_weekly_top_up_channels"
            ),

            # node(
            #     billing_most_popular_top_up_channel_weekly,
            #     ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_for_l2_billing_and_payments_weekly_most_popular_top_up_channel",
            #      "params:l2_most_popular_topup_channel"],
            #     "l2_billing_and_payments_weekly_most_popular_top_up_channel"
            # ),

            node(
                billing_most_popular_top_up_channel_weekly,
                [
                    "l1_billing_and_payments_daily_most_popular_top_up_channel_for_l2_billing_and_payments_weekly_most_popular_top_up_channel",
                    "l0_billing_topup_type_for_l2_billing_and_payments_weekly_most_popular_top_up_channel",
                    "params:l2_popular_top_up_channel",
                    "params:l2_most_popular_topup_channel",
                    "params:exception_partition_list_for_l1_billing_and_payments_daily_most_popular_top_up_channel_for_l2_billing_and_payments_weekly_most_popular_top_up_channel"
                ],
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate", "l2_billing_and_payments_weekly_most_popular_top_up_channel"]
            ),

            # Weekly last top up channel pre-paid
            node(
                billing_last_top_up_channel_weekly,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l2_billing_and_payments_weekly_last_top_up_channel",
                 "l1_customer_profile_union_daily_feature_for_l2_billing_and_payments_weekly_last_top_up_channel",
                 "l0_billing_topup_type_for_l2_billing_and_payments_weekly_last_top_up_channel",
                 "params:l2_last_topup_channel",
                 "params:exception_partition_list_for_l0_billing_and_payments_rt_t_recharge_daily_for_l2_billing_and_payments_weekly_last_top_up_channel",
                 "params:exception_partition_list_for_l1_customer_profile_union_daily_feature_for_l2_billing_and_payments_weekly_last_top_up_channel"
                 ],
                "l2_billing_and_payments_weekly_last_top_up_channel"
            ),

            # node(
            #     billing_popular_topup_day_weekly,
            #     ["l2_billing_and_payments_weekly_popular_topup_day_intermediate_for_l2_billing_and_payments_weekly_popular_topup_day",
            #      "params:l2_popular_topup_day_2"],
            #     "l2_billing_and_payments_weekly_popular_topup_day"
            # ),

            node(
                billing_popular_topup_day_weekly,
                [
                    "l1_billing_and_payments_daily_popular_topup_day_for_l2_billing_and_payments_weekly_popular_topup_day",
                    "params:l2_popular_topup_day_1",
                    "params:l2_popular_topup_day_2",
                    "params:exception_partition_list_for_l1_billing_and_payments_daily_popular_topup_day_for_l2_billing_and_payments_weekly_popular_topup_day"
                ],
                ["l2_billing_and_payments_weekly_popular_topup_day_intermediate", "l2_billing_and_payments_weekly_popular_topup_day"]
            ),

            # node(
            #     billing_popular_topup_hour_weekly,
            #     ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate_for_l2_billing_and_payments_weekly_popular_topup_hour",
            #      "params:l2_popular_topup_hour_2"],
            #     "l2_billing_and_payments_weekly_popular_topup_hour"
            # ),

            node(
                billing_popular_topup_hour_weekly,
                [
                    "l1_billing_and_payments_daily_popular_topup_day_for_l2_billing_and_payments_weekly_popular_topup_hour",
                    "params:l2_popular_topup_hour_1",
                    "params:l2_popular_topup_hour_2",
                    "params:exception_partition_list_for_l1_billing_and_payments_daily_popular_topup_day_for_l2_billing_and_payments_weekly_popular_topup_hour"
                ],
                ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate", "l2_billing_and_payments_weekly_popular_topup_hour"]
            ),

            # Weekly time since last top up  pre-paid
            node(
                billing_time_since_last_topup_weekly,
                ["l1_billing_and_payments_daily_time_since_last_top_up_for_l2_billing_and_payments_weekly_time_since_last_top_up",
                 "params:l2_time_since_last_top_up",
                 "params:exception_partition_list_for_l1_billing_and_payments_daily_time_since_last_top_up_for_l2_billing_and_payments_weekly_time_since_last_top_up"
                 ],
                "l2_billing_and_payments_weekly_time_since_last_top_up"
            ),

            # Weekly last 3 top up volume pre-paid
            node(
                df_copy_for_l2_billing_and_payments_weekly_last_three_topup_volume,
                ["l1_billing_and_payments_daily_time_since_last_top_up_for_l2_billing_and_payments_weekly_last_three_topup_volume",
                "params:exception_partition_list_for_l1_billing_and_payments_daily_time_since_last_top_up_for_l2_billing_and_payments_weekly_last_three_topup_volume"],
                "int_l2_billing_and_payments_weekly_last_three_topup_volume_1"
            ),
            node(
                node_from_config,
                ["int_l2_billing_and_payments_weekly_last_three_topup_volume_1",
                 "params:l2_last_three_topup_volume_ranked"],
                "l2_billing_and_payments_weekly_last_three_topup_volume_1"
            ),
            node(
                billing_last_three_topup_volume_weekly,
                ["l2_billing_and_payments_weekly_last_three_topup_volume_1",
                 "params:l2_last_three_topup_volume"],
                "l2_billing_and_payments_weekly_last_three_topup_volume"
            ),
        ]
    )
