from functools import partial

from kedro.pipeline import Pipeline, node
from datetime import timedelta
from src.nba.report.nodes.report_nodes import *


def create_campaign_view_report_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_campaign_view_report,
                    aggregate_period=[1, 7, 30],
                    day="2020-03-01",
                ),
                inputs={
                    "campaign_response_input_table": "campaign_response_input_table",
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                    "reporting_kpis": "reporting_kpis",
                },
                outputs="campaign_view_report_tbl",
                name="campaign_view",
                tags=["campaign_view"],
            ),
        ]
    )


def create_use_case_view_report_data() -> Pipeline:
    mock_report_running_date = "2020-04-25"
    mock_report_running_date_list = [
        "2020-04-21",
        "2020-04-22",
        "2020-04-23",
        "2020-04-24",
        "2020-04-25",
    ]
    start_date = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
        days=-120
    )
    start_date_input = datetime.strptime(
        mock_report_running_date, "%Y-%m-%d"
    ) + timedelta(days=-30)
    return Pipeline(
        [
            # node(
            #     create_use_case_campaign_mapping_table,
            #     {
            #         "campaign_churn_cvm_master": "campaign_churn_cvm_master",
            #         "campaign_churn_bau_master": "campaign_churn_bau_master",
            #         "campaign_ard_cvm_master": "campaign_ard_cvm_master",
            #     },
            #     outputs="use_case_campaign_mapping",
            #     name="create_use_case_campaign_mapping_table",
            #     tags=["create_use_case_campaign_mapping_table",],
            # ),
            node(
                partial(
                    create_report_campaign_tracking_table,
                    date_from=start_date_input,
                    date_to=mock_report_running_date,
                ),
                {
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "l0_campaign_tracking_contact_list_pre": "l0_campaign_tracking_contact_list_pre",
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                },
                "campaign_response_input_table",
                name="campaign_response_input_table",
                tags=["campaign_response_input_table",],
            ),
            # node(
            #     partial(
            #         create_input_data_for_reporting_kpis,
            #         date_from=start_date_input,  # TODO make dynamic
            #         date_to=datetime.strptime(
            #             mock_report_running_date, "%Y-%m-%d"
            #         ),  # TODO make dynamic
            #     ),
            #     inputs={
            #         "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
            #         "dm42_promotion_prepaid": "dm42_promotion_prepaid",
            #         "dm43_promotion_prepaid": "dm43_promotion_prepaid",
            #         "dm01_fin_top_up": "dm01_fin_top_up",
            #         "dm15_mobile_usage_aggr_prepaid": "dm15_mobile_usage_aggr_prepaid",
            #         "dm07_sub_clnt_info":"dm07_sub_clnt_info",
            #         "prepaid_no_activity_daily": "prepaid_no_activity_daily",
            #     },
            #     outputs="reporting_kpis_input",
            #     name="create_input_data_for_reporting_kpis",
            #     tags=["create_input_data_for_reporting_kpis"],
            # ),
            # node(
            #     partial(
            #         node_reporting_kpis,
            #         date_from=start_date,  # TODO make dynamic
            #         date_to=datetime.strptime(
            #             mock_report_running_date, "%Y-%m-%d"
            #         ),  # TODO make dynamic
            #         arpu_days_agg_periods=[1, 7, 30],
            #     ),
            #     inputs={"reporting_kpis_input": "reporting_kpis_input",},
            #     outputs="reporting_kpis",
            #     name="reporting_kpis",
            #     tags=["reporting_kpis"],
            # ),
            # node(
            #     node_daily_kpis_by_group_report,
            #     inputs={"reporting_kpis": "reporting_kpis",},
            #     outputs="daily_kpis_by_group_report",
            #     name="daily_kpis_by_group_report",
            #     tags=["daily_kpis_by_group_report"],
            # ),
            # node(
            #     node_plot_daily_kpis_by_group_report,
            #     inputs={"daily_kpis_by_group_report": "daily_kpis_by_group_report",},
            #     outputs="plot_daily_kpis_by_group_report",
            #     name="plot_daily_kpis_by_group_report",
            #     tags=["plot_daily_kpis_by_group_report"],
            # ),
            # node(
            #     partial(
            #         create_use_case_view_report,
            #         day_list=mock_report_running_date_list,  # TODO make dynamic
            #         aggregate_period=[1, 7, 30],
            #         dormant_days_agg_periods=[5, 7, 14, 30, 60, 90],
            #     ),
            #     inputs={
            #         "use_case_campaign_mapping": "use_case_campaign_mapping",
            #         "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
            #         "campaign_response_input_table": "campaign_response_input_table",
            #         "reporting_kpis": "reporting_kpis",
            #         "reporting_kpis_input": "reporting_kpis_input",
            #     },
            #     outputs="use_case_view_report_table",
            #     name="use_case_view_report_table",
            #     tags=["use_case_view_report_table",],
            # ),
            # node(
            #     partial(store_historical_usecase_view_report),
            #     inputs={"use_case_view_report_table": "use_case_view_report_table",},
            #     outputs="historical_use_case_view_report_table",
            #     name="historical_use_case_view_report_table",
            #     tags=["historical_use_case_view_report_table",],
            # ),
        ],
        tags=["churn_ard_report"],
    )
