from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *


def billing_l1_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            # Monthly top up count and top up volume pre-paid
            node(
                billing_topup_count_and_volume_node_monthly,
                ["l1_billing_and_payments_daily_topup_and_volume_for_l3_billing_and_payments_monthly_topup_and_volume",
                 "params:l3_billing_and_payment_feature_top_up_and_count_monthly"],
                "l3_billing_and_payments_monthly_topup_and_volume"
            ),


            # Monthly arpu of roaming post-paid
            node(
                billing_arpu_roaming_node_monthly,
                ["l1_billing_and_payments_daily_rpu_roaming_for_l3_billing_and_payments_monthly_rpu_roaming",
                 "params:l3_billing_and_payment_feature_rpu_roaming_monthly"],
                "l3_billing_and_payments_monthly_rpu_roaming"
            ),

            # Monthly before top up balance feature pre-paid
            node(
                billing_before_topup_balance_node_monthly,
                ["l1_billing_and_payments_daily_before_top_up_balance_for_l3_billing_and_payments_monthly_before_top_up_balance",
                 "params:l3_billing_and_payment_before_top_up_balance_monthly"],
                "l3_billing_and_payments_monthly_before_top_up_balance"
            ),

            # Monthly top up channels feature pre-paid
            node(
                billing_topup_channels_node_monthly,
                ["l1_billing_and_payments_daily_top_up_channels_for_l3_billing_and_payments_monthly_top_up_channels",
                 "params:l3_billing_and_payment_top_up_channels_monthly"],
                "l3_billing_and_payments_monthly_top_up_channels"
            ),

            # Monthly most popular top up channel feature pre-paid
            # New version: 2020-10-21
            node(
                l3_billing_and_payments_monthly_most_popular_top_up_channel, [
                    "l1_billing_and_payments_daily_most_popular_top_up_channel_for_l3_billing_and_payments_monthly_most_popular_top_up_channel",
                    "l0_billing_topup_type_for_l3_billing_and_payments_monthly_most_popular_top_up_channel",
                    "params:l3_popular_topup_channel",
                    "params:l3_most_popular_topup_channel"
                ],
                "l3_billing_and_payments_monthly_most_popular_top_up_channel"
            ),
            # Old version by MCK
            # node(
            #     top_up_channel_joined_data_for_monthly_most_popular_top_up_channel,
            #     ["l1_billing_and_payments_daily_most_popular_top_up_channel_for_l3_billing_and_payments_monthly_most_popular_top_up_channel",
            #      "l0_billing_topup_type_for_l3_billing_and_payments_monthly_most_popular_top_up_channel"],
            #     "l3_billing_and_payments_monthly_most_popular_top_up_channel_1"
            # ),
            # node(
            #     node_from_config,
            #     ["l3_billing_and_payments_monthly_most_popular_top_up_channel_1",
            #      "params:l3_popular_topup_channel"],
            #     "l3_billing_and_payments_monthly_most_popular_top_up_channel_2"
            # ),
            # node(
            #     billing_most_popular_topup_channel_monthly,
            #     ["l3_billing_and_payments_monthly_most_popular_top_up_channel_2",
            #      "params:l3_most_popular_topup_channel"],
            #     "l3_billing_and_payments_monthly_most_popular_top_up_channel"
            # ),

            # Monthly favourite top up channel feature pre-paid
            # Update : 2021-05-13
            node(
                l3_billing_and_payment_monthly_favourite_topup_channal, [
                    "l1_billing_and_payment_monthly_favourite_topup_channal_for_l3_billing_and_payment_monthly_favourite_topup_channal",
                    "l0_billing_topup_type_for_l3_billing_and_payment_monthly_favourite_topup_channal",
                    "params:l3_favourite_topup_channel",
                    "params:l3_favourite_topup_channel_rank"
                ],
                "l3_billing_and_payment_monthly_favourite_topup_channal"
            ),

            # Monthly popular top up day feature pre-paid
            node(
                copy_df_for_l3_billing_and_payments_monthly_popular_topup_day,
                "l1_billing_and_payments_daily_popular_topup_day_for_l3_billing_and_payments_monthly_popular_topup_day",
                "int_l3_billing_and_payments_monthly_popular_topup_day_1"
            ),
            node(
                node_from_config,
                ["int_l3_billing_and_payments_monthly_popular_topup_day_1",
                 "params:l3_popular_topup_day_ranked"],
                "l3_billing_and_payments_monthly_popular_topup_day_1"
            ),
            node(
                billing_popular_topup_day_monthly,
                ["l3_billing_and_payments_monthly_popular_topup_day_1",
                 "params:l3_popular_topup_day"],
                "l3_billing_and_payments_monthly_popular_topup_day"
            ),

            # Monthly popular top up hour feature pre-paid
            node(
                copy_df_for_l3_billing_and_payments_monthly_popular_topup_hour,
                "l1_billing_and_payments_daily_popular_topup_day_for_l3_billing_and_payments_monthly_popular_topup_hour",
                "int_l3_billing_and_payments_monthly_popular_topup_hour_1"
            ),
            node(
                node_from_config,
                ["int_l3_billing_and_payments_monthly_popular_topup_hour_1",
                 "params:l3_popular_topup_hour_ranked"],
                "l3_billing_and_payments_monthly_popular_topup_hour_1"
            ),
            node(
                billing_popular_topup_hour_monthly,
                ["l3_billing_and_payments_monthly_popular_topup_hour_1",
                 "params:l3_popular_topup_hour"],
                "l3_billing_and_payments_monthly_popular_topup_hour"
            ),

            # Monthly time since last top up feature pre-paid
            node(
                billing_time_since_last_topup_node_monthly,
                ["l1_billing_and_payments_daily_time_since_last_top_up_for_l3_billing_and_payments_monthly_time_since_last_top_up",
                 "params:l3_time_since_last_top_up"],
                "l3_billing_and_payments_monthly_time_since_last_top_up"
            ),

            # Monthly last 3 top up volume pre-paid
            node(
                copy_df_for_l3_billing_and_payments_monthly_last_three_topup_volume,
                "l1_billing_and_payments_daily_time_since_last_top_up_for_l3_billing_and_payments_monthly_last_three_topup_volume",
                "int_l3_billing_and_payments_monthly_last_three_topup_volume_1"
            ),
            node(
                node_from_config,
                ["int_l3_billing_and_payments_monthly_last_three_topup_volume_1",
                 "params:l3_last_three_topup_volume_ranked"],
                "l3_billing_and_payments_monthly_last_three_topup_volume_1"
            ),
            node(
                billing_last_three_topup_volume_monthly,
                ["l3_billing_and_payments_monthly_last_three_topup_volume_1",
                 "params:l3_last_three_topup_volume"],
                "l3_billing_and_payments_monthly_last_three_topup_volume"
            ),
        ]
    )


def billing_l0_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            # Join monthly billing data with customer profile
            node(
                billing_rpu_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_rpu",
                 "l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly_for_l3_billing_and_payments_monthly_rpu"],
                "billing_monthly_data"
            ),

            # Monthly arpu vas,gprs,voice feature pre-paid,post-paid both
            node(
                billing_arpu_node_monthly,
                ["billing_monthly_data",
                 "params:l3_billing_and_payment_revenue_per_user_monthly"],
                "l3_billing_and_payments_monthly_rpu"
            ),

            # Monthly time difference between top ups pre-paid
            node(
                copy_df_for_l3_billing_and_payments_monthly_topup_time_diff,
                "l0_billing_and_payments_rt_t_recharge_daily_for_l3_billing_and_payments_monthly_topup_time_diff",
                "Int_l3_billing_and_payments_monthly_topup_diff_time_intermediate"
            ),
            node(
                node_from_config,
                ["Int_l3_billing_and_payments_monthly_topup_diff_time_intermediate",
                 "params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly_intermdeiate"],
                "l3_billing_and_payments_monthly_topup_diff_time_intermediate"
            ),
            node(
                billing_time_diff_between_topups_monthly,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_topup_time_diff",
                 "l3_billing_and_payments_monthly_topup_diff_time_intermediate",
                 "params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly"],
                "l3_billing_and_payments_monthly_topup_time_diff"
            ),

            # Monthly automated payment feature post-paid
            node(
                bill_payment_daily_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_automated_payments",
                 "l0_billing_pc_t_payment_daily_for_l3_billing_and_payments_monthly_automated_payments"],
                "l3_billing_monthly_automated_payments_1"
            ),
            node(
                node_from_config,
                ["l3_billing_monthly_automated_payments_1",
                 "params:l3_automated_flag"],
                "l3_billing_and_payments_monthly_automated_payments"
            ),


            # Join monthly billing statement hist data with customer profile
            node(
                billing_statement_hist_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_bill_volume",
                 "l0_billing_statement_history_monthly_for_l3_billing_and_payments_monthly_bill_volume", "params:l3_billing_and_payments_monthly_bill_volume_tbl"],
                "billing_stat_hist_monthly_data"
            ),

            # Monthly volume of bill and roaming bills post-paid
            node(
                billing_volume_of_bills_and_roaming_bills_monthly,
                ["billing_stat_hist_monthly_data",
                 "params:l3_bill_volume"],
                "l3_billing_and_payments_monthly_bill_volume"
            ),

            node(
                billing_statement_hist_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_missed_bills",
                 "l0_billing_statement_history_monthly_for_l3_billing_and_payments_monthly_missed_bills", "params:l3_billing_and_payments_monthly_missed_bills_tbl"],
                "billing_stat_hist_monthly_data_missed_bills_stg"
            ),
            # Monthly missed bills feature post-paid
            node(
                billing_data_joined,
                ["billing_stat_hist_monthly_data_missed_bills_stg",
                 "l0_billing_pc_t_payment_daily_for_l3_billing_and_payments_monthly_missed_bills", "params:l3_billing_and_payments_monthly_missed_bills_tbl"],
                "l3_billing_and_payments_monthly_joined_missed_bills_stg"
            ),
            node(
                billing_missed_bills_monthly,
                ["l3_billing_and_payments_monthly_joined_missed_bills_stg",
                 "params:l3_missed_bills"],
                "l3_billing_and_payments_monthly_missed_bills"
            ),

            node(
                billing_statement_hist_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_overdue_bills",
                 "l0_billing_statement_history_monthly_for_l3_billing_and_payments_monthly_overdue_bills", "params:l3_billing_and_payments_monthly_overdue_bills_tbl"],
                "billing_stat_hist_monthly_data_overdue_bills_stg"
            ),
            node(
                billing_data_joined,
                ["billing_stat_hist_monthly_data_overdue_bills_stg",
                 "l0_billing_pc_t_payment_daily_for_l3_billing_and_payments_monthly_overdue_bills", "params:l3_billing_and_payments_monthly_overdue_bills_tbl"],
                "l3_billing_and_payments_monthly_joined_overdue_bills_stg"
            ),
            # Monthly overdue bills feature post-paid
            node(
                billing_overdue_bills_monthly,
                ["l3_billing_and_payments_monthly_joined_overdue_bills_stg",
                 "params:l3_overdue_bills"],
                "l3_billing_and_payments_monthly_overdue_bills"
            ),

            node(
                billing_statement_hist_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume",
                 "l0_billing_statement_history_monthly_for_l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume", "params:l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume_tbl"],
                "billing_stat_hist_monthly_data_last_overdue_bill_days_ago_and_volume_stg"
            ),

            node(
                billing_data_joined,
                ["billing_stat_hist_monthly_data_last_overdue_bill_days_ago_and_volume_stg",
                 "l0_billing_pc_t_payment_daily_for_l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume", "params:l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume_tbl"],
                "l3_billing_and_payments_monthly_joined_last_overdue_bill_days_ago_and_volume__stg"
            ),

            # Monthly last overdue bill volume and days ago feature post-paid
            node(
                billing_last_overdue_bill_volume_monthly,
                ["l3_billing_and_payments_monthly_joined_last_overdue_bill_days_ago_and_volume__stg",
                 "params:l3_last_overdue_bill_days_ago_and_volume"],
                "l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume"
            ),
        #
            #Monthly last top up channel pre-paid
            node(
                billing_last_topup_channel_monthly,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l3_billing_and_payments_monthly_last_top_up_channel",
                 "l3_customer_profile_include_1mo_non_active_for_l3_billing_and_payments_monthly_last_top_up_channel",
                 "l0_billing_topup_type_for_l3_billing_and_payments_monthly_last_top_up_channel",
                 "params:l3_last_topup_channel"],
                "l3_billing_and_payments_monthly_last_top_up_channel"
            ),
        #
        #     # Feature roaming billing volume add IR package
        #     # Filter product and ppu roaming volume
        #     node(
        #         int_l3_billing_and_payments_monthly_roaming_bill_volume,
        #         ["l0_billing_statement_charge_hist_monthly_for_l3_billing_and_payments_monthly_roaming_bill_volume",
        #          "l0_product_ir_package"
        #          ],
        #         "int_l3_billing_and_payments_monthly_roaming_bill_volume"
        #     ),
        #
        #     # Sum ppu roaming volume
        #     node(
        #         node_from_config,
        #         ["int_l3_billing_and_payments_monthly_roaming_bill_volume",
        #          "params:l3_billing_and_payments_monthly_roaming_bill_volume_ppu"],
        #         "l3_billing_and_payments_monthly_roaming_bill_volume_ppu"
        #     ),
        #
        #     # Sum package roaming volume
        #     node(
        #         node_from_config,
        #         ["int_l3_billing_and_payments_monthly_roaming_bill_volume",
        #          "params:l3_billing_and_payments_monthly_roaming_bill_volume_package"],
        #         "l3_billing_and_payments_monthly_roaming_bill_volume_package"
        #     ),
        #
        #     # Sum total roaming volume
        #     node(
        #         l3_billing_and_payments_monthly_roaming_bill_volume,
        #         ["l3_billing_and_payments_monthly_roaming_bill_volume_package",
        #          "l3_billing_and_payments_monthly_roaming_bill_volume_ppu",
        #          "params:l3_billing_and_payments_monthly_roaming_bill_volume"],
        #         "l3_billing_and_payments_monthly_roaming_bill_volume"
        #     ),
        ]
    )

