from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *


def billing_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            # Monthly top up count and top up volume
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_topup_and_volume",
                 "params:l3_billing_and_payment_feature_top_up_and_count_monthly"],
                "l3_billing_and_payments_monthly_topup_and_volume"
            ),

            # Monthly arpu vas,gprs,voice feature
            node(
                node_from_config,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:l3_billing_and_payment_revenue_per_user_monthly"],
                "l3_billing_and_payments_monthly_rpu"
            ),

            # Monthly time difference between top ups

            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly_intermediate"],
                "l3_billing_and_payments_monthly_topup_time_diff_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_topup_time_diff_1",
                 "params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly"],
                "l3_billing_and_payments_monthly_topup_time_diff"
            ),

            # Monthly arpu of roaming
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_rpu_roaming",
                 "params:l3_billing_and_payment_feature_rpu_roaming_monthly"],
                "l3_billing_monthly_rpu_roaming"
            ),

            # Monthly automated payment feature
            node(
                derive_month_automated_payment,
                ["l0_billing_pc_t_payment_daily"],
                "l3_billing_monthly_automated_payments_1"
            ),
            node(
                node_from_config,
                ["l3_billing_monthly_automated_payments_1",
                 "params:l3_automated_flag"],
                "l3_billing_monthly_automated_payments"
            ),

            # Monthly before top up balance feature
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_before_top_up_balance",
                 "params:l3_billing_and_payment_before_top_up_balance_monthly"],
                "l3_billing_and_payments_monthly_before_top_up_balance"
            ),

            # Monthly top up channels feature
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_top_up_channels",
                 "params:l3_billing_and_payment_top_up_channels_monthly"],
                "l3_billing_and_payments_monthly_top_up_channels"
            ),

            # Monthly most popular top up channel feature
            node(
                top_up_channel_joined_data,
                ["l1_billing_and_payments_daily_most_popular_top_up_channel",
                 "l0_billing_topup_type"],
                "l3_billing_and_payments_monthly_most_popular_top_up_channel_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_most_popular_top_up_channel_1",
                 "params:l3_popular_topup_channel"],
                "l3_billing_and_payments_monthly_most_popular_top_up_channel_2"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_most_popular_top_up_channel_2",
                 "params:l3_most_popular_topup_channel"],
                "l3_billing_and_payments_monthly_most_popular_top_up_channel"
            ),

            # Monthly volume of bill and roaming bills
            node(
                derive_month_bill_volume,
                ["l0_billing_statement_history_monthly"],
                "l3_billing_and_payments_monthly_bill_volume_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_bill_volume_1",
                 "params:l3_bill_volume"],
                "l3_billing_and_payments_monthly_bill_volume"
            ),

            # Monthly last top up channel

            node(
                top_up_channel_joined_data,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "l0_billing_topup_type"],
                "l3_billing_and_payments_monthly_last_top_up_channel_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_last_top_up_channel_1",
                 "params:l3_last_topup_channel_ranked"],
                "l3_billing_and_payments_monthly_last_top_up_channel_2"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_last_top_up_channel_2",
                 "params:l3_last_topup_channel"],
                "l3_billing_and_payments_monthly_last_top_up_channel"
            ),

            # Monthly missed bills feature
            node(
                billing_data_joined,
                ["l0_billing_statement_history_monthly",
                 "l0_billing_pc_t_payment_daily"],
                "l3_billing_and_payments_monthly_joined"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_joined",
                 "params:l3_missed_bills"],
                "l3_billing_and_payments_monthly_missed_bills"
            ),

            # Monthly overdue bills feature
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_joined",
                 "params:l3_overdue_bills"],
                "l3_billing_and_payments_monthly_overdue_bills"
            ),

            # Monthly last overdue bill volume and days ago feature
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_joined",
                 "params:l3_last_overdue_bill_days_ago_and_volume"],
                "l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume"
            ),

            # Monthly popular top up day feature
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_popular_topup_day",
                 "params:l3_popular_topup_day_ranked"],
                "l3_billing_and_payments_monthly_popular_topup_day_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_popular_topup_day_1",
                 "params:l3_popular_topup_day"],
                "l3_billing_and_payments_monthly_popular_topup_day"
            ),

            # Monthly popular top up hour feature
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_popular_topup_day",
                 "params:l3_popular_topup_hour_ranked"],
                "l3_billing_and_payments_monthly_popular_topup_hour_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_popular_topup_hour_1",
                 "params:l3_popular_topup_hour"],
                "l3_billing_and_payments_monthly_popular_topup_hour"
            ),

            # Monthly time since last top up feature
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_time_since_last_top_up",
                 "params:l3_time_since_last_top_up"],
                "l3_billing_and_payments_monthly_time_since_last_top_up"
            ),

            # Monthly last 3 top up volume
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_time_since_last_top_up",
                 "params:l3_last_three_topup_volume_ranked"],
                "l3_billing_and_payments_monthly_last_three_topup_volume_1"
            ),
            node(
                node_from_config,
                ["l3_billing_and_payments_monthly_last_three_topup_volume_1",
                 "params:l3_last_three_topup_volume"],
                "l3_billing_and_payments_monthly_last_three_topup_volume"
            ),
        ]
    )