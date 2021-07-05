from functools import partial

from du.model_input.model_input_nodes import fix_analytic_id_key
from du.models.package_prefer_nodes import (
    create_daily_ontop_pack,
    create_aggregate_ontop_package_preference_input,
    create_ontop_package_preference,
)
from du.scoring.scoring_nodes import (
    l5_scoring_profile,
    l5_du_scored,
    l5_du_scored_new_experiment,
    du_join_preference_new,
    du_union_scoring_output,
)
from kedro.pipeline import Pipeline, node
from nba.model_input.model_input_nodes import (
    node_l5_nba_customer_profile,
)
from nba.pcm_scoring.pcm_scoring_nodes import join_c360_features_latest_date

PROD_SCHEMA_NAME = "prod_dataupsell"
DEV_SCHEMA_NAME = "dev_dataupsell"


def create_package_preference_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_daily_ontop_pack,
                    hive_table="l1_data_ontop_purchase_daily_hive",
                    start_date=None,
                    end_date=None,
                    drop_replace_partition=True,
                ),
                inputs={
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "ontop_pack": "dm42_promotion_prepaid",
                    "usage_feature": "dm15_mobile_usage_aggr_prepaid",
                },
                outputs="unused_memory_dataset_1",
                name="l1_data_ontop_purchase_daily",
                tags=["package_preference_data", "l1_data_ontop_purchase_daily"],
            ),
            node(
                partial(
                    create_aggregate_ontop_package_preference_input,
                    hive_table="l4_data_ontop_purchase_week_hive_aggregate_feature",
                    aggregate_periods=[90, 60, 30],
                    start_date=None,
                    drop_replace_partition=True,
                ),
                inputs={
                    "l1_data_ontop_purchase_daily": "l1_data_ontop_purchase_daily_hive",
                    "unused_memory_dataset_1": "unused_memory_dataset_1",
                },
                outputs="unused_memory_dataset_2",
                name="l4_data_ontop_purchase_week_hive_aggregate_feature",
                tags=[
                    "package_preference_data",
                    "l4_data_ontop_purchase_week_hive_aggregate_feature",
                ],
            ),
            node(
                partial(
                    create_ontop_package_preference,
                    hive_table="l4_data_ontop_package_preference",
                    aggregate_periods=[90, 60, 30],
                    start_date=None,
                    drop_replace_partition=True,
                ),
                inputs={
                    "l4_data_ontop_purchase_week_hive_aggregate_feature": "l4_data_ontop_purchase_week_hive_aggregate_feature",
                    "unused_memory_dataset_2": "unused_memory_dataset_2",
                },
                outputs="unused_memory_dataset_3",
                name="l4_data_ontop_package_preference",
                tags=["package_preference_data", "l4_data_ontop_package_preference"],
            ),
        ],
        tags="package_preference_pipeline",
    )


def create_du_scoring_input_pipeline() -> Pipeline:
    return Pipeline(
        [
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
                l5_scoring_profile,
                inputs={
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                },
                outputs="l5_du_eligible_sub_to_score",
                name="l5_scoring_profile",
                tags=["l5_scoring_profile"],
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
                join_c360_features_latest_date,
                inputs={
                    "df_spine": "l5_du_eligible_sub_to_score",
                    # "unused_memory_fix_id":"unused_memory_fix_id",
                    "subset_features": "params:du_model_input_features",
                    "l5_nba_customer_profile": "l5_du_customer_profile",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    # "l4_streaming_visit_count_and_download_traffic_feature": "l4_streaming_visit_count_and_download_traffic_feature",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "l4_macro_product_purchase_feature_weekly_key_fixed": "l4_macro_product_purchase_feature_weekly_key_fixed",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features"
                },
                outputs="l5_du_scoring_master",
                name="l5_du_scoring_master",
                tags=["l5_du_scoring_master"],
            ),
        ],
        tags="du_scoring_input_pipeline",
    )


def create_du_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            # Scoring Reference Group
            node(
                l5_du_scored,
                inputs={
                    "df_master": "l5_du_scoring_master",
                    "dataupsell_usecase_control_group_table": "dataupsell_usecase_control_group_table",
                    "control_group": "params:du_sandbox_groupname_reference",
                    "l5_average_arpu_untie_lookup": "l5_average_arpu_untie_lookup",
                    "model_group_column": "params:du_model_scoring_group_column",
                    "explanatory_features": "params:du_model_features_reference",
                    "acceptance_model_tag": "params:du_acceptance_model_tag",
                    "mlflow_model_version": "params:du_mlflow_model_version_prediction_reference",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "scoring_chunk_size": "params:du_scoring_chunk_size",
                },
                outputs="unused_memory_du_scored1",
                name="l5_du_score_reference",
                tags=["l5_du_scored"],
            ),
            node(
                l5_du_scored,
                inputs={
                    "df_master": "l5_du_scoring_master",
                    "dataupsell_usecase_control_group_table": "dataupsell_usecase_control_group_table",
                    "control_group": "params:du_sandbox_groupname_bau",
                    "l5_average_arpu_untie_lookup": "l5_average_arpu_untie_lookup",
                    "model_group_column": "params:du_model_scoring_group_column",
                    "explanatory_features": "params:du_model_features_bau",
                    "acceptance_model_tag": "params:du_acceptance_model_tag",
                    "mlflow_model_version": "params:du_mlflow_model_version_prediction_bau",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "scoring_chunk_size": "params:du_scoring_chunk_size",
                },
                outputs="unused_memory_du_scored2",
                name="l5_du_score_bau",
                tags=["l5_du_scored"],
            ),
            node(
                l5_du_scored_new_experiment,
                inputs={
                    "df_master": "l5_du_scoring_master",
                    "dataupsell_usecase_control_group_table": "dataupsell_usecase_control_group_table",
                    "control_group": "params:du_sandbox_groupname_new_experiment",
                    "model_group_column": "params:du_model_scoring_group_column",
                    "top_features_path": "params:du_top_features_path",
                    "acceptance_model_tag": "params:du_acceptance_model_tag",
                    "mlflow_model_version": "params:du_mlflow_model_version_prediction_new_experiment",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "scoring_chunk_size": "params:du_scoring_chunk_size"
                },
                outputs="unused_memory_du_scored3",
                name="l5_du_score_new_experiment",
                tags=["l5_du_scored"],
            ),
            node(
                du_union_scoring_output,
                inputs={
                    "du_sandbox_groupname_bau": "params:du_sandbox_groupname_bau",
                    "du_sandbox_groupname_new_experiment": "params:du_sandbox_groupname_new_experiment",
                    "du_sandbox_groupname_reference": "params:du_sandbox_groupname_reference",
                },
                outputs="unused_memory_du_scored",
                name="du_union_scoring_output",
                tags=["l5_du_scored"],
            ),
            # node(
            #     validate_model_scoring,
            #     inputs={
            #         "df_master": "l5_du_scoring_master",
            #         "explanatory_features": "params:du_model_explanatory_features",
            #     },
            #     outputs="unused_memory_validate",
            #     name="validate_models",
            #     tags=["validate_models"],
            # )
        ],
        tags="du_scoring_pipeline",
    )


def create_du_scored_join_package_preference_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    du_join_preference_new,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_scored": "l5_du_scored",
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l5_du_scoring_master": "l5_du_scoring_master",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                },
                outputs="unused_memory_du_preference",
                name="l5_du_join_preference",
                tags=["du_join_preference"],
            ),
        ]
    )


def create_du_scored_join_package_preference_pipeline_dev() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    du_join_preference_new,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_scored": "l5_du_scored",
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l5_du_scoring_master": "l5_du_scoring_master",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                },
                outputs="unused_memory_dataset_4",
                name="l5_du_join_preference",
                tags=["du_join_preference"],
            ),
        ]
    )