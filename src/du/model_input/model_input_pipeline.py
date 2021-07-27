from functools import partial

from kedro.pipeline import Pipeline, node

from du.model_input.model_input_nodes import (
    node_l5_du_target_variable_table_new,
    node_l5_du_target_variable_table_disney,
    node_l5_du_target_variable_table,
    node_l5_du_master_spine_table,
    node_l5_du_master_table_chunk_debug_acceptance,
    node_l5_du_master_table_only_accepted,
    fix_analytic_id_key,
)
from du.model_input.create_campaign_mapping import create_mannual_campaign_mapping

from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table,
    node_l5_nba_customer_profile,
)


def create_du_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                create_mannual_campaign_mapping,
                inputs={
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "path_to_ngcm_deploy_plan": "params:path_to_ngcm_deploy_plan",
                },
                outputs="mapping_for_model_training",
                name="mapping_for_model_training",
                tags=["l5_du_target_variable_tbl"],
            ),
            node(
                node_l5_nba_customer_profile,
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                },
                outputs="l5_du_customer_profile",
                name="l5_du_customer_profile",
                tags=["l5_du_customer_profile"],
            ),
            node(
                partial(
                    node_l5_du_target_variable_table_new, starting_date="2021-04-15",
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "mapping_for_model_training": "mapping_for_model_training",
                },
                outputs="l5_du_target_variable_tbl",
                name="l5_du_target_variable_tbl",
                tags=["l5_du_target_variable_tbl"],
            ),
            node(
                partial(
                    node_l5_du_target_variable_table_disney, starting_date="2021-04-15",
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "mapping_for_model_training": "mapping_for_model_training",
                    "l5_du_target_variable_tbl": "l5_du_target_variable_tbl"
                },
                outputs="l5_du_target_variable_with_disney_tbl",
                name="node_l5_du_target_variable_table_disney",
                tags=["node_l5_du_target_variable_table_disney"],
            ),
            node(
                partial(node_l5_du_master_spine_table, min_feature_days_lag=5, ),
                inputs={
                    "l5_du_target_variable_tbl": "l5_du_target_variable_with_disney_tbl",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                },
                outputs="l5_du_master_spine_tbl",
                name="l5_du_master_spine_tbl",
                tags=["l5_du_master_spine_tbl"],
            ),

            node(
                fix_analytic_id_key,
                inputs={
                    "l4_macro_product_purchase_feature_weekly": "l4_macro_product_purchase_feature_weekly",
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info",
                },
                outputs="unused_memory_fix_id",
                name="fix_l4_analytic_id",
                tags=["fix_l4_analytic_id"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:du_model_input_features",
                    "l5_nba_master_table_spine": "l5_du_master_spine_tbl",
                    "l5_du_customer_profile": "l5_du_customer_profile",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_macro_product_purchase_feature_weekly_key_fixed": "l4_macro_product_purchase_feature_weekly_key_fixed",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "digital_persona_prepaid_monthly_production": "digital_persona_prepaid_monthly_production"
                },
                outputs="l5_du_master_tbl",
                name="l5_du_master_tbl",
                tags=["l5_du_master_tbl", "du_masters"],
            ),
            node(
                node_l5_du_master_table_only_accepted,
                inputs={"l5_du_master_table": "l5_du_master_tbl"},
                outputs="l5_du_master_table_only_accepted",
                name="l5_du_master_table_only_accepted",
                tags=["l5_du_master_table_only_accepted", "du_masters"],
            ),
            # node(
            #     partial(
            #         node_l5_du_master_table_chunk_debug_acceptance,
            #         group_target="Data_NonStop_4Mbps_1_ATL",
            #         sampling_rate=1e-4,
            #     ),
            #     inputs={"l5_du_master_table": "l5_du_master_tbl",},
            #     outputs=[
            #         "l5_du_master_table_chunk_debug_acceptance",
            #         "master_table_chunk_debug_extra_pai_metrics_acceptance_du",
            #     ],
            #     name="l5_du_master_table_chunk_debug_acceptance",
            #     tags=["l5_du_master_table_chunk_debug_acceptance",],
            # ),

        ]
    )
