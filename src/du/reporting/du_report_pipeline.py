from kedro.pipeline import Pipeline, node
from functools import partial
import datetime
from du.reporting.revenue_uplift_nodes import (
    l5_du_weekly_revenue_uplift_report_contacted_only,
    l5_du_weekly_revenue_uplift_report_overall_contacted,
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
            node(
                partial(
                    l5_du_weekly_revenue_uplift_report_contacted_only,
                    control_group_initialize_profile_date="2020-08-01",
                ),
                inputs={
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                    "l0_du_pre_experiment3_groups": "l0_du_pre_experiment5_groups",
                    "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l4_revenue_prepaid_pru_f_usage_multi_features": "l4_revenue_prepaid_pru_f_usage_multi_features",
                    "l0_product_pru_m_ontop_master_for_weekly_full_load":"l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "dm42_promotion_prepaid":"dm42_promotion_prepaid",
                },
                outputs="l5_du_weekly_revenue_uplift_report_contacted_only",
                name="create_du_weekly_revenue_uplift_report",
                tags=["create_du_weekly_revenue_uplift_report",],
            ),
        ]
    )
