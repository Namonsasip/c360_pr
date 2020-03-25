from functools import partial
from kedro.pipeline import Pipeline, node
from src.nba.report.nodes.report_nodes import *

# Ankit commented this out as it is not being used and getting import exception
#from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix

def create_use_case_view_report_data(run_type: str = None) -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_report_campaign_tracking_table,
                    day="2020-03-03",  # TODO make dynamic
                ),
                {
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "dm996_cvm_ontop_pack": "dm996_cvm_ontop_pack",
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                    "report_create_campaign_tracking_table_parameters": "params:report_create_campaign_tracking_table",
                },
                "campaign_response_input_table",
                name="campaign_response_input_table",
                tags=["campaign_response_input_table",],
            ),
            node(
                partial(
                    node_reporting_kpis,
                    date_from=datetime.strptime(
                        "2020-02-01", "%Y-%m-%d"
                    ),  # TODO make dynamic
                    date_to=datetime.strptime(
                        "2020-03-03", "%Y-%m-%d"
                    ),  # TODO make dynamic
                    arpu_days_agg_periods=[1, 7, 30],
                    dormant_days_agg_periods=[5, 7, 14, 30, 60, 90],
                ),
                inputs={
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "dm42_promotion_prepaid": "dm42_promotion_prepaid",
                    "dm43_promotion_prepaid": "dm43_promotion_prepaid",
                    "dm01_fin_top_up": "dm01_fin_top_up",
                    "dm15_mobile_usage_aggr_prepaid": "dm15_mobile_usage_aggr_prepaid",
                    "prepaid_no_activity_daily": "prepaid_no_activity_daily",
                },
                outputs="reporting_kpis",
                name="reporting_kpis",
                tags=["reporting_kpis"],
            ),
            node(
                node_daily_kpis_by_group_report,
                inputs={"reporting_kpis": "reporting_kpis",},
                outputs="daily_kpis_by_group_report",
                name="daily_kpis_by_group_report",
                tags=["daily_kpis_by_group_report"],
            ),
            node(
                node_plot_daily_kpis_by_group_report,
                inputs={"daily_kpis_by_group_report": "daily_kpis_by_group_report",},
                outputs="plot_daily_kpis_by_group_report",
                name="plot_daily_kpis_by_group_report",
                tags=["plot_daily_kpis_by_group_report"],
            ),
            node(
                partial(
                    create_use_case_view_report,
                    day="2020-03-03",  # TODO make dynamic
                    aggregate_period=[1, 7, 30],
                ),
                inputs={
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "campaign_response_input_table": "campaign_response_input_table",
                    "reporting_kpis": "reporting_kpis",
                },
                outputs="use_case_view_report_table",
                name="use_case_view_report_table",
                tags=["use_case_view_report_table",],
            ),
        ],
        tags=["churn_ard_report"],
    )
