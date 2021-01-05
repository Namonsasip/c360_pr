from functools import partial

from kedro.pipeline import Pipeline, node
from datetime import timedelta
from src.nba.reporting.nodes.report_nodes import (
    create_gcg_marketing_performance_pre_data,
)
from datetime import datetime
from src.nba.reporting.nodes.postpaid_report_nodes import (
    create_gcg_marketing_performance_post_data,
)


def create_gcg_marketing_performance_report_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                create_gcg_marketing_performance_post_data,
                inputs={
                    "l3_campaign_postpaid_prepaid_monthly": "l3_campaign_postpaid_prepaid_monthly",
                    "l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                    "l3_customer_profile_union_monthly_feature": "l3_customer_profile_union_monthly_feature",
                    "l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly": "l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info",
                    "profile_customer_profile_post": "profile_customer_profile_post",
                },
                outputs="unused_memory_dataset0",
                name="create_gcg_marketing_performance_post_data",
                tags=["nba_report", "gcg"],
            ),
            node(
                create_gcg_marketing_performance_pre_data,
                inputs={
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                    "l2_customer_profile_union_weekly_feature": "l2_customer_profile_union_weekly_feature",
                    "l1_revenue_prepaid_pru_f_usage_multi_daily": "l1_revenue_prepaid_pru_f_usage_multi_daily",
                    "prepaid_no_activity_daily": "prepaid_no_activity_daily",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info",
                },
                outputs="unused_memory_dataset",
                name="create_gcg_marketing_performance_pre_data",
                tags=["nba_report", "gcg"],
            ),
        ]
    )
