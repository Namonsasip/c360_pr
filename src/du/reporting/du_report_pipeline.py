from kedro.pipeline import Pipeline, node
from functools import partial
import datetime
from du.reporting.revenue_uplift_nodes import (
    l5_du_weekly_revenue_uplift_report_contacted_only,
    l5_du_weekly_revenue_uplift_report_overall_contacted,
    l5_du_monthly_revenue_uplift_report,
)

from du.reporting.report_cmm import (
    l5_data_upsell_ontop_revenue_weekly_report,
    l5_data_upsell_ontop_revenue_weekly_report_tg_cg,
    l5_data_upsell_churn_ontop_revenue_weekly_report,
    l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs,
    l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg,
    l5_data_upsell_ontop_revenue_weekly_report_group_combine_hs,
l5_data_upsell_ontop_revenue_weekly_report_group_sandbox_combine_hs
)


def create_du_weekly_revenue_uplift_report_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     partial(
            #         l5_du_weekly_revenue_uplift_report_overall_contacted,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "l0_du_pre_experiment3_groups": "l0_du_pre_experiment5_groups",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l4_revenue_prepaid_pru_f_usage_multi_features":"l4_revenue_prepaid_pru_f_usage_multi_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #     },
            #     outputs="l5_du_weekly_revenue_uplift_report_overall_contacted",
            #     name="l5_du_weekly_revenue_uplift_report_overall_contacted",
            #     tags=["l5_du_weekly_revenue_uplift_report_overall_contacted",],
            # ),
            # node(
            #     partial(
            #         l5_du_weekly_revenue_uplift_report_contacted_only,
            #         control_group_initialize_profile_date="2020-08-01",
            #     ),
            #     inputs={
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "l0_du_pre_experiment3_groups": "l0_du_pre_experiment5_groups",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l4_revenue_prepaid_pru_f_usage_multi_features": "l4_revenue_prepaid_pru_f_usage_multi_features",
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "dm42_promotion_prepaid": "dm42_promotion_prepaid",
            #     },
            #     outputs="l5_du_weekly_revenue_uplift_report_contacted_only",
            #     name="create_du_weekly_revenue_uplift_report",
            #     tags=["create_du_weekly_revenue_uplift_report",],
            # ),
            # node(
            #     partial(
            #         l5_du_monthly_revenue_uplift_report,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_full_load",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "mapping_for_model_training":"mapping_for_model_training",
            #         "dm42_promotion_prepaid": "dm42_promotion_prepaid",
            #     },
            #     outputs="l5_du_monthly_revenue_uplift_report",
            #     name="l5_du_monthly_revenue_uplift_report",
            #     tags=["l5_du_monthly_revenue_uplift_report",],
            # ),
            # node(
            #     partial(
            #         l5_data_upsell_ontop_revenue_weekly_report,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
            #     },
            #     outputs="l5_data_upsell_ontop_revenue_weekly_report",
            #     name="l5_data_upsell_ontop_revenue_weekly_report",
            #     tags=["l5_data_upsell_ontop_revenue_weekly_report",],
            # ),
            # node(
            #     partial(
            #         l5_data_upsell_ontop_revenue_weekly_report_tg_cg,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
            #     },
            #     outputs="l5_data_upsell_ontop_revenue_weekly_report_tg_cg",
            #     name="l5_data_upsell_ontop_revenue_weekly_report_tg_cg",
            #     tags=["l5_data_upsell_ontop_revenue_weekly_report_tg_cg",],
            # ),
            # node(
            #     partial(
            #         l5_data_upsell_churn_ontop_revenue_weekly_report,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l0_campaign_tracking_campaign_response_master": "l0_campaign_tracking_campaign_response_master",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
            #     },
            #     outputs="l5_data_upsell_churn_ontop_revenue_weekly_report",
            #     name="l5_data_upsell_churn_ontop_revenue_weekly_report",
            #     tags=["l5_data_upsell_churn_ontop_revenue_weekly_report",],
            # ),
            # node(
            #     partial(
            #         l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l0_campaign_tracking_campaign_response_master": "l0_campaign_tracking_campaign_response_master",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
            #     },
            #     outputs="l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg",
            #     name="l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg",
            #     tags=["l5_data_upsell_churn_ontop_revenue_weekly_report_tg_cg",],
            # ),
            node(
                partial(
                    l5_data_upsell_ontop_revenue_weekly_report_group_sandbox_combine_hs,
                    control_group_initialize_profile_date="2021-05-01",
                    owner_name="Vitita Herabat",
                ),
                inputs={
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                    "dm42_promotion_prepaid": "dm42_promotion_prepaid",
                    "mapping_for_model_training": "mapping_for_model_training",
                    "control_group_tbl": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
                },
                outputs="l5_data_upsell_ontop_revenue_weekly_report_group_combine_hs",
                name="l5_data_upsell_ontop_revenue_weekly_report_group_combine_hs",
                tags=["l5_data_upsell_ontop_revenue_weekly_report_group_combine_hs",],
            ),
            # node(
            #     partial(
            #         l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs,
            #         control_group_initialize_profile_date="2020-08-01",
            #         owner_name="Vitita Herabat",
            #     ),
            #     inputs={
            #         "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
            #         "l0_revenue_pru_f_ontop_pospre_daily_full_load": "l0_revenue_pru_f_ontop_pospre_daily_full_load",
            #         "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
            #         "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #         "mapping_for_model_training": "mapping_for_model_training",
            #         "control_group_tbl": "l0_du_pre_experiment5_groups",
            #         "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
            #     },
            #     outputs="l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs",
            #     name="l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs",
            #     tags=["l5_data_upsell_ontop_revenue_weekly_report_tg_cg_combine_hs",],
            # ),
        ]
    )
