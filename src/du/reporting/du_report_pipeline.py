from kedro.pipeline import Pipeline, node
from functools import partial
import datetime
from du.reporting.revenue_uplift_nodes import (
    l5_du_weekly_revenue_uplift_report_contacted_only,
    l5_du_weekly_revenue_uplift_report_contacted_only_old,
)


def create_du_weekly_revenue_uplift_report_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    l5_du_weekly_revenue_uplift_report_contacted_only,
                    control_group_initialize_profile_date="2020-08-01",
                ),
                inputs={
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                    "l0_du_pre_experiment3_groups": "l0_du_pre_experiment3_groups",
                    "l3_customer_profile_union_monthly_feature_full_load": "l3_customer_profile_union_monthly_feature_full_load",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                },
                outputs="l5_du_weekly_revenue_uplift_report_contacted_only",
                name="create_du_weekly_revenue_uplift_report",
                tags=["create_du_weekly_revenue_uplift_report",],
            ),
        ]
    )
