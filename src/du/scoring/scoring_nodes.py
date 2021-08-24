import datetime
import logging
import time

import mlflow
from customer360.utilities.spark_util import get_spark_session
from du.models.models_nodes import score_du_models, score_du_models_new_experiment
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from kedro.io import CSVLocalDataSet


def format_time(elapsed):
    """
    Takes a time in seconds and returns a string hh:mm:ss
    """
    # Round to the nearest second.
    elapsed_rounded = int(round((elapsed)))

    # Format as hh:mm:ss
    return str(datetime.timedelta(seconds=elapsed_rounded))


# get latest available daily profile from c360 feature
def l5_scoring_profile(
        l1_customer_profile_union_daily_feature_full_load: DataFrame,
) -> DataFrame:
    df_latest_sub_id_mapping = l1_customer_profile_union_daily_feature_full_load.withColumn(
        "aux_date_order",
        F.row_number().over(
            Window.partitionBy("old_subscription_identifier").orderBy(
                F.col("event_partition_date").desc()
            )
        ),
    )
    df_latest_sub_id_mapping = df_latest_sub_id_mapping.filter(
        F.col("aux_date_order") == 1
    ).drop("aux_date_order")
    df_latest_sub_id_mapping = df_latest_sub_id_mapping.select(
        "subscription_identifier", "charge_type",
    )
    df_latest_sub_id_mapping = df_latest_sub_id_mapping.where(
        "charge_type = 'Pre-paid'"
    ).drop("charge_type")

    df_latest_sub_id_mapping = (
        df_latest_sub_id_mapping.withColumn(
            "today", F.lit(datetime.datetime.date(datetime.datetime.now()))
        )
            .withColumn("day_of_week", F.dayofweek("today"))
            .withColumn("day_of_month", F.dayofmonth("today"))
            .drop("today")
    )

    return df_latest_sub_id_mapping


def l5_du_scored(
        df_master: DataFrame,
        dataupsell_usecase_control_group_table: DataFrame,
        control_group: str,
        l5_average_arpu_untie_lookup: DataFrame,
        model_group_column: str,
        explanatory_features,
        acceptance_model_tag: str,
        mlflow_model_version,
        arpu_model_tag: str,
        delta_table_schema: str,
        scoring_chunk_size: int = 500000,
        **kwargs,
):
    # Data upsell generate score for every possible upsell campaign
    spark = get_spark_session()
    df_master = df_master.join(
        dataupsell_usecase_control_group_table.drop("register_date").where(
            "usecase_control_group LIKE '" + control_group + "%'"
        ),
        ["old_subscription_identifier"],
        "inner",
    )
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='regression' AND params.Able_to_model = 'True' AND params.Version='"
                      + str(mlflow_model_version)
                      + "'",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )
    all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    # df_master = catalog.load("l5_du_scoring_master")
    eligible_model = mlflow_sdf.selectExpr(model_group_column)
    df_master_upsell = df_master.crossJoin(F.broadcast(eligible_model))

    df_master_upsell = df_master_upsell.withColumn(
        "du_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("register_date"),
            F.lit("_"),
            F.col(model_group_column),
        ),
    )

    df_master_scored = score_du_models(
        df_master=df_master_upsell,
        primary_key_columns=["subscription_identifier", ],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        explanatory_features=explanatory_features,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )
    # df_master_scored = df_master_scored.join(df_master_upsell, ["du_spine_primary_key"], how="left")
    df_master_scored.write.format("delta").mode("overwrite").saveAsTable(
        delta_table_schema + ".l5_du_scored_" + control_group
    )
    return df_master_scored


def l5_disney_scored(
        df_master: DataFrame,
        to_score_validation_set: bool,
        disney_cg_tg_group_table: DataFrame,
        model_group_column: str,
        feature_importance_binary_model,
        acceptance_model_tag: str,
        mlflow_model_version,
        delta_table_schema: str,
        scoring_chunk_size: int = 500000,
        **kwargs,
):
    spark = get_spark_session()
    # feature_importance_binary_model_list = feature_importance_binary_model[
    #     "feature"
    # ].to_list()

    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string=f"""tags.mlflow.runName='DisneyPlusHotstar' 
                            AND params.model_objective='binary' 
                            AND params.Able_to_model = 'True' AND params.Version='{mlflow_model_version}'""",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )
    all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    eligible_model = mlflow_sdf.selectExpr(model_group_column)

    if not to_score_validation_set:
        df_master = df_master.join(
            disney_cg_tg_group_table.where("usecase_control_group LIKE '%TG' AND usecase_control_group != 'GCG'"),
            on=["old_subscription_identifier", "subscription_identifier", "access_method_num"],
            how="inner",
        )
    else:
        df_master = df_master.where('old_subscription_identifier is not null')

    if model_group_column in df_master.columns:
        df_master_upsell = df_master
    else:
        df_master_upsell = df_master.crossJoin(F.broadcast(eligible_model))

    df_master_upsell = df_master_upsell.dropDuplicates(["old_subscription_identifier"])
    df_master_upsell = df_master_upsell.withColumn(
        "du_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col(model_group_column),
        ),
    )
    feature_importance_binary_model_list = ['age', 'norms_net_revenue',
                                            'charge_type_numeric',
                                            'network_type_numeric',
                                            'mobile_segment_numeric',
                                            'subscription_status_numeric',
                                            'cust_active_this_month_numeric',
                                            'sum_payments_top_ups_last_week',
                                            'sum_payments_top_ups_last_four_week',
                                            'sum_payments_top_ups_last_twelve_week',
                                            'sum_payments_arpu_last_month',
                                            'sum_payments_arpu_last_three_month',
                                            'sum_payments_arpu_gprs_last_month',
                                            'sum_payments_arpu_gprs_last_three_month',
                                            'sum_payments_arpu_vas_last_month',
                                            'sum_payments_arpu_vas_last_three_month',
                                            'sum_payments_arpu_voice_last_month',
                                            'sum_payments_arpu_voice_last_three_month',
                                            'payments_one_month_over_three_month_dynamics_arpu',
                                            'sum_payments_arpu_roaming_weekly_last_week',
                                            'sum_payments_arpu_roaming_weekly_last_twelve_week',
                                            'sum_payments_before_top_up_balance_weekly_last_week',
                                            'sum_payments_before_top_up_balance_weekly_last_four_week',
                                            'sum_payments_top_ups_by_bank_atm_cdm_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_twelve_week',
                                            'sum_payments_top_ups_by_cash_card_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_cash_card_weekly_last_twelve_week',
                                            'sum_payments_top_ups_by_digital_online_self_service_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_twelve_week',
                                            'sum_payments_top_ups_by_epin_slip_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_epin_slip_weekly_last_twelve_week',
                                            'sum_payments_top_ups_by_epos_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_epos_weekly_last_twelve_week',
                                            'sum_payments_top_ups_by_rom_weekly_last_twelve_week',
                                            'sum_payments_top_up_vol_by_rom_weekly_last_twelve_week',
                                            'sum_payments_top_ups_daily_last_seven_day',
                                            'sum_payments_top_ups_daily_last_thirty_day',
                                            'sum_payments_top_ups_daily_last_ninety_day',
                                            'avg_campaign_total_others_by_sms_sum_weekly_last_twelve_week',
                                            'sum_campaign_total_by_sms_sum_weekly_last_four_week_over_twelve_weeks',
                                            'sum_campaign_total_others_by_sms_sum_weekly_last_four_week_over_twelve_weeks',
                                            'avg_campaign_overall_count_sum_weekly_last_twelve_week',
                                            'avg_campaign_total_others_by_ussd_sum_weekly_last_twelve_week',
                                            'avg_campaign_total_eligible_by_sms_sum_weekly_last_twelve_week',
                                            'sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week_over_twelve_weeks',
                                            'avg_campaign_overall_count_sum_weekly_last_four_week',
                                            'avg_campaign_total_others_by_myais_app_sum_weekly_last_twelve_week',
                                            'sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week_over_twelve_weeks',
                                            'sum_campaign_total_others_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks',
                                            'avg_campaign_total_upsell_xsell_by_sms_sum_weekly_last_twelve_week',
                                            'sum_device_most_used_handset_apple_weekly_last_week',
                                            'sum_device_most_used_handset_oppo_weekly_last_week',
                                            'sum_device_most_used_handset_samsung_weekly_last_week',
                                            'max_device_high_range_weekly_last_week',
                                            'max_device_mid_range_weekly_last_week',
                                            'max_device_low_range_weekly_last_week',
                                            'max_device_smartphone_weekly_last_week',
                                            'max_device_standardphone_weekly_last_week',
                                            'max_device_legacyphone_weekly_last_week',
                                            'sum_rev_arpu_total_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_revenue_monthly_last_three_month',
                                            'sum_rev_arpu_total_gprs_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_sms_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_others_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_voice_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_mms_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_ir_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_idd_net_revenue_monthly_last_month',
                                            'sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month',
                                            'sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month',
                                            'sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month',
                                            'sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month',
                                            'sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month',
                                            'sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month',
                                            'sum_usg_incoming_total_call_duration_daily_last_ninety_day',
                                            'sum_usg_incoming_total_call_duration_daily_last_seven_day',
                                            'sum_usg_outgoing_data_volume_daily_last_fourteen_day',
                                            'sum_usg_outgoing_data_volume_daily_last_ninety_day',
                                            'sum_usg_outgoing_data_volume_daily_last_seven_day',
                                            'sum_usg_outgoing_total_call_duration_daily_last_ninety_day',
                                            'sum_usg_outgoing_total_call_duration_daily_last_seven_day',
                                            'sum_usg_outgoing_total_sms_daily_last_ninety_day',
                                            'sum_usg_outgoing_total_sms_daily_last_seven_day',
                                            'number_of_transaction_fixed_speed_BTL_2_week',
                                            'number_of_transaction_fixed_speed_ATL_2_week',
                                            'number_of_transaction_full_speed_ATL_2_week',
                                            'total_net_tariff_fixed_speed_BTL_2_week',
                                            'total_net_tariff_fixed_speed_ATL_2_week',
                                            'total_net_tariff_full_speed_ATL_2_week',
                                            'number_of_transaction_fixed_speed_BTL_4_week',
                                            'number_of_transaction_full_speed_BTL_4_week',
                                            'number_of_transaction_fixed_speed_ATL_4_week',
                                            'number_of_transaction_full_speed_ATL_4_week',
                                            'total_net_tariff_fixed_speed_BTL_4_week',
                                            'total_net_tariff_fixed_speed_ATL_4_week',
                                            'total_net_tariff_full_speed_ATL_4_week',
                                            'number_of_transaction_1_day_validity_2_week',
                                            'number_of_transaction_7_day_validity_2_week',
                                            'number_of_transaction_30_day_validity_2_week',
                                            'total_net_tariff_1_day_validity_2_week',
                                            'total_net_tariff_7_day_validity_2_week',
                                            'total_net_tariff_30_day_validity_2_week',
                                            'number_of_transaction_1_day_validity_4_week',
                                            'number_of_transaction_7_day_validity_4_week',
                                            'number_of_transaction_30_day_validity_4_week',
                                            'total_net_tariff_1_day_validity_4_week',
                                            'total_net_tariff_7_day_validity_4_week',
                                            'total_net_tariff_30_day_validity_4_week',
                                            'sum_usg_incoming_data_volume_sum_weekly_last_week',
                                            'sum_usg_incoming_data_volume_sum_weekly_last_four_week',
                                            'sum_usg_incoming_data_volume_sum_weekly_last_twelve_week',
                                            'sum_usg_incoming_data_volume_2G_3G_sum_weekly_last_four_week',
                                            'sum_usg_incoming_data_volume_4G_sum_weekly_last_four_week',
                                            'sum_usg_incoming_local_data_volume_sum_weekly_last_four_week',
                                            'sum_usg_incoming_local_data_volume_2G_3G_sum_weekly_last_four_week',
                                            'sum_usg_incoming_local_data_volume_4G_sum_weekly_last_four_week',
                                            'sum_usg_incoming_roaming_data_volume_sum_weekly_last_twelve_week',
                                            'sum_usg_outgoing_data_volume_sum_weekly_last_four_week',
                                            'sum_usg_total_data_volume_sum_weekly_last_week',
                                            'sum_usg_total_data_volume_sum_weekly_last_four_week',
                                            'sum_usg_total_data_volume_sum_weekly_last_twelve_week',
                                            'sum_usg_incoming_roaming_call_duration_sum_weekly_last_week',
                                            'sum_usg_incoming_roaming_call_duration_sum_weekly_last_twelve_week',
                                            'sum_usg_incoming_local_call_duration_sum_weekly_last_week',
                                            'sum_usg_incoming_local_call_duration_sum_weekly_last_four_week',
                                            'sum_usg_incoming_local_call_duration_sum_weekly_last_twelve_week',
                                            'sum_usg_incoming_total_sms_sum_weekly_last_week',
                                            'sum_usg_incoming_total_sms_sum_weekly_last_twelve_week',
                                            'sum_usg_incoming_weekday_calls_duration_sum_weekly_last_four_week',
                                            'sum_usg_incoming_weekday_number_calls_sum_weekly_last_four_week',
                                            'sum_usg_incoming_weekend_calls_duration_sum_weekly_last_four_week',
                                            'sum_usg_incoming_weekend_number_calls_sum_weekly_last_four_week',
                                            'sum_usg_outgoing_total_call_duration_sum_weekly_last_week',
                                            'sum_usg_outgoing_total_call_duration_sum_weekly_last_four_week',
                                            'sum_usg_outgoing_total_call_duration_sum_weekly_last_twelve_week',
                                            'sum_usg_outgoing_total_sms_sum_weekly_last_week',
                                            'sum_usg_outgoing_total_sms_sum_weekly_last_four_week',
                                            'sum_usg_outgoing_total_sms_sum_weekly_last_twelve_week',
                                            'video_streaming_scoring',
                                            'social_network_scoring',
                                            'communication_scoring',
                                            'financial_service_scoring',
                                            'game_scoring',
                                            'food_delivery_scoring',
                                            'investment_scoring',
                                            'online_shopping_scoring',
                                            'traveller_scoring',
                                            'working_scoring',
                                            'ais_service_scoring',
                                            'music_scoring',
                                            'news_scoring',
                                            'wearable_devices_scoring',
                                            'sport_scoring',
                                            'life_style_dining_scoring',
                                            'digital_cluster']  # TODO Edit this code or simply place here if want to see features

    df_master_scored = score_du_models_new_experiment(
        df_master=df_master_upsell,
        primary_key_columns=["subscription_identifier"],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
        },
        scoring_chunk_size=scoring_chunk_size,
        feature_importance_binary_model=feature_importance_binary_model_list,
        feature_importance_regression_model=None,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )

    logging.warning("SCORE SUCCESSFULLY")
    # df_master_scored = df_master_scored.join(df_master_upsell, ["du_spine_primary_key"], how="left")

    if to_score_validation_set:
        logging.warning("Saving to table")
        df_master_scored.write.format("delta").mode("overwrite").saveAsTable(
            delta_table_schema + ".disney_validation_set_scored"
        )
    else:
        logging.warning("Saving to table")
        df_master_scored.write.format("delta").mode("overwrite").saveAsTable(
            delta_table_schema + ".disney_target_group_scored"
        )
    return df_master_scored


def l5_du_scored_new_experiment(
        df_master: DataFrame,
        dataupsell_usecase_control_group_table: DataFrame,
        control_group: str,
        model_group_column: str,
        feature_importance_binary_model,
        feature_importance_regression_model,
        acceptance_model_tag: str,
        mlflow_model_version,
        arpu_model_tag: str,
        delta_table_schema: str,
        scoring_chunk_size: int = 500000,
        **kwargs,
):
    # Data upsell generate score for every possible upsell campaign
    spark = get_spark_session()
    feature_importance_binary_model_list = feature_importance_binary_model[
        "feature"
    ].to_list()
    feature_importance_regression_model_list = feature_importance_regression_model[
        "feature"
    ].to_list()
    df_master = df_master.join(
        dataupsell_usecase_control_group_table.drop("register_date").where(
            "usecase_control_group LIKE '" + control_group + "%'"
        ),
        ["old_subscription_identifier"],
        "inner",
    )
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='regression' AND params.Able_to_model = 'True' AND params.Version='"
                      + str(mlflow_model_version)
                      + "'",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )
    all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    eligible_model = mlflow_sdf.selectExpr(model_group_column)
    df_master_upsell = df_master.crossJoin(F.broadcast(eligible_model))

    df_master_upsell = df_master_upsell.withColumn(
        "du_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("register_date"),
            F.lit("_"),
            F.col(model_group_column),
        ),
    )

    df_master_scored = score_du_models_new_experiment(
        df_master=df_master_upsell,
        primary_key_columns=["subscription_identifier"],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        feature_importance_binary_model=feature_importance_binary_model_list,
        feature_importance_regression_model=feature_importance_regression_model_list,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )
    # logging.warning(f"RESULT HAS {df_master_scored.count()} ROWS")
    logging.warning("SCORE SUCCESSFULLY")
    # df_master_scored = df_master_scored.join(df_master_upsell, ["du_spine_primary_key"], how="left")
    df_master_scored.write.format("delta").mode("overwrite").saveAsTable(
        delta_table_schema + ".l5_du_scored_" + control_group
    )
    return df_master_scored


def du_union_scoring_output(
        du_sandbox_groupname_bau,
        du_sandbox_groupname_new_experiment,
        du_sandbox_groupname_reference,
        delta_table_schema,
        unused_memory_du_scored1,
        unused_memory_du_scored2,
        unused_memory_du_scored3,
):
    spark = get_spark_session()
    df_master_scored = spark.sql(
        f"SELECT * FROM {delta_table_schema}.l5_du_scored_" + du_sandbox_groupname_bau
    )
    df_master_scored = df_master_scored.union(
        spark.sql(
            f"SELECT * FROM {delta_table_schema}.l5_du_scored_"
            + du_sandbox_groupname_reference
        )
    )
    df_master_scored = df_master_scored.union(
        spark.sql(
            f"SELECT * FROM {delta_table_schema}.l5_du_scored_"
            + du_sandbox_groupname_new_experiment
        )
    )
    df_master_scored.write.format("delta").mode("overwrite").saveAsTable(
        delta_table_schema + ".l5_du_scored"
    )
    return df_master_scored


def du_join_preference_new(
        l5_du_scored: DataFrame,
        l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
        l5_du_scoring_master: DataFrame,
        l4_data_ontop_package_preference: DataFrame,
        mode,
        delta_table_schema,
):
    spark = get_spark_session()
    t0 = time.time()
    l5_du_scored = l5_du_scored.withColumn(
        "scoring_day",
        F.lit(
            datetime.datetime.date(
                datetime.datetime.now() + datetime.timedelta(hours=7)
            )
        ),
    )
    l5_du_scoring_master = l5_du_scoring_master.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "access_method_num",
        "register_date",
        "day_of_week",
        "day_of_month",
        "subscription_status",
        "age",
        "subscriber_tenure",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "sum_rev_arpu_total_revenue_monthly_last_three_month",
        "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month",
        "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month",
    )

    l5_du_scored = l5_du_scored.join(
        l5_du_scoring_master, ["subscription_identifier"], "left"
    )

    # Only Select Pre-paid Charge type, Convert partition_date to date format
    master_ontop_weekly = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.where(
            "charge_type = 'Prepaid'"
        )
            .withColumn(
            "partition_date_str",
            l0_product_pru_m_ontop_master_for_weekly_full_load["partition_date"].cast(
                StringType()
            ),
        )
            .drop("partition_date")
            .select(
            "price_inc_vat",
            "package_type",
            "promotion_code",
            "package_group",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "package_name_report",
            "data_quota",
            "duration",
            F.to_timestamp("partition_date_str", "yyyyMMdd").alias(
                "partition_date_timestamp"
            ),
        )
            .selectExpr("*", "date(partition_date_timestamp) as partition_date")
            .drop("partition_date_timestamp")
    )

    # Cleansing Master data
    ##
    # Data Quota are transformed from string data to float value of MB, All unlimited are impute with extreme value
    # while null are impute with -1
    ##
    # Data Speed are inferred by it speed, the unit use is Kbps
    ##
    # This weekly data is always updated on Wednesday, However C360 Weekly Data Define start of week by Monday
    # So, we -2 partition_date to get start_of_week for data mapping
    master_ontop_weekly_fixed = master_ontop_weekly.selectExpr(
        "price_inc_vat",
        "package_type",
        "promotion_code",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        """CASE WHEN data_quota LIKE '%GB%' THEN split(data_quota, 'GB')[0] * 1024
                WHEN data_quota LIKE '%MB%' THEN split(data_quota, 'MB')[0]
                WHEN data_quota LIKE '%Hr%' THEN 999999999
                WHEN data_quota LIKE '%D' THEN 999999999
                WHEN data_quota LIKE '%M%' THEN 999999999
                WHEN data_quota LIKE '%Mins%' THEN 999999999
                WHEN data_quota LIKE '%UL%' THEN 999999999
                ELSE -1 END AS data_quota_mb""",
        """CASE WHEN duration LIKE '%D%' THEN split(duration, 'D')[0] 
                                        WHEN duration LIKE '%Hr%' THEN split(duration, 'Hr')[0]/24
                                     ELSE 0 END AS duration """,
        """CASE WHEN mm_data_Speed = 'Entertain' THEN 4096
            WHEN mm_data_Speed = 'Social' THEN 512
            WHEN mm_data_Speed = 'Time' THEN 51200
            WHEN mm_data_Speed = 'Others' THEN 512
            WHEN MM_Data_Speed = '64Kbps'				 THEN    64
            WHEN MM_Data_Speed = '256Kbps'			 THEN   256
      WHEN MM_Data_Speed = '384Kbps'			 THEN   384
      WHEN MM_Data_Speed = '512Kbps'			 THEN   512
      WHEN MM_Data_Speed = '1Mbps'				 THEN  1024
      WHEN MM_Data_Speed = '2Mbps'  			 THEN  2048
      WHEN MM_Data_Speed = '4Mbps'				 THEN  4096
      WHEN MM_Data_Speed = '6Mbps'		    	 THEN  6144
      WHEN MM_Data_Speed = '10Mbps'				 THEN 10240
      WHEN MM_Data_Speed = 'Full speed'			 THEN 51200
      WHEN MM_Data_Speed = 'Full Speed'		     THEN 51200
      WHEN MM_Data_Speed = 'Full Speed - Next G' THEN 51200
      WHEN MM_Data_Speed = '7.2Mbps'             THEN 7372
            ELSE 0
            END as data_speed""",
        "DATE_ADD(partition_date,-2) as start_of_week",
    )
    max_master_date = (
        master_ontop_weekly_fixed.withColumn("G", F.lit(1))
            .groupby("G")
            .agg(F.max("start_of_week"))
            .collect()
    )

    agg_master_ontop = (
        master_ontop_weekly_fixed.where(
            "start_of_week = date('"
            + datetime.datetime.strftime(max_master_date[0][1], "%Y-%m-%d")
            + "')"
        )
            .groupby(
            "package_name_report",
            "package_type",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "data_quota_mb",
            "duration",
            "data_speed",
        )
            .agg(F.count("*").alias("CNT"), F.max("price_inc_vat").alias("price_inc_vat"))
            .drop("CNT")
    )
    agg_master_ontop = agg_master_ontop.selectExpr(
        "package_name_report as offer_package_name_report",
        "package_type as offer_package_type",
        "mm_types as offer_mm_types",
        "mm_data_speed as offer_mm_data_speed",
        "data_quota_mb as offer_data_quota_mb",
        "duration as offer_duration",
        "data_speed as offer_data_speed",
        "price_inc_vat as offer_price_inc_vat",
    )
    l5_du_scored_info = l5_du_scored.join(
        agg_master_ontop.withColumn(
            "model_name",
            F.regexp_replace(
                "offer_package_name_report", "(\.\/|\/|\.|\+|\-|\(|\)|\ )", "_"
            ),
        ),
        ["model_name"],
        "left",
    )
    max_package_preference_date = (
        l4_data_ontop_package_preference.withColumn("G", F.lit(1))
            .groupby("G")
            .agg(F.max("start_of_week"))
            .collect()
    )
    l5_du_scored_offer_preference = (
        l5_du_scored_info.selectExpr("*", "date(register_date) as register_date_d")
            .drop("register_date")
            .withColumnRenamed("register_date_d", "register_date")
            .join(
            l4_data_ontop_package_preference.drop("access_method_num").where(
                "start_of_week = date('"
                + datetime.datetime.strftime(
                    max_package_preference_date[0][1], "%Y-%m-%d"
                )
                + "')"
            ),
            ["old_subscription_identifier", "register_date"],
            "left",
        )
    )

    l5_du_scored_offer_preference = l5_du_scored_offer_preference.dropDuplicates(
        ["old_subscription_identifier", "model_name"]
    )

    if mode == "Development":
        spark.sql(
            f"""DROP TABLE IF EXISTS 
            {delta_table_schema}.du_offer_score_with_package_preference_rework"""
        )
        l5_du_scored_offer_preference.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            f"""CREATE TABLE {delta_table_schema}.du_offer_score_with_package_preference_rework
            USING DELTA
            AS 
            SELECT * FROM tmp_tbl"""
        )
    else:  # Production
        spark.sql(
            f"DELETE FROM {delta_table_schema}.du_offer_score_with_package_preference_rework WHERE scoring_day = date('"
            + datetime.datetime.strftime(
                datetime.datetime.now() + datetime.timedelta(hours=7), "%Y-%m-%d",
            )
            + "')"
        )
        l5_du_scored_offer_preference.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(
            f"{delta_table_schema}.du_offer_score_with_package_preference_rework"
        )
    elapsed = format_time(time.time() - t0)
    logging.warning("Node du_join_preference took: {:}".format(elapsed))

    return l5_du_scored_offer_preference


def du_join_preference(
        l5_du_scored: DataFrame,
        mapping_for_model_training: DataFrame,
        l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
        l5_du_scoring_master: DataFrame,
        l4_data_ontop_package_preference: DataFrame,
        schema_name,
        prod_schema_name,
        dev_schema_name,
):
    spark = get_spark_session()
    t0 = time.time()
    # l5_du_scored = catalog.load("l5_du_scored")
    # l5_du_scoring_master = catalog.load("l5_du_scoring_master")
    #
    # mapping_for_model_training = catalog.load("mapping_for_model_training")
    # l0_product_pru_m_ontop_master_for_weekly_full_load = catalog.load(
    #     "l0_product_pru_m_ontop_master_for_weekly_full_load"
    # )
    # l4_data_ontop_package_preference = catalog.load("l4_data_ontop_package_preference")

    l5_du_scored = l5_du_scored.withColumn(
        "scoring_day",
        F.lit(
            datetime.datetime.date(
                datetime.datetime.now() + datetime.timedelta(hours=7)
            )
        ),
    )
    l5_du_scoring_master = l5_du_scoring_master.selectExpr(
        "subscription_identifier",
        "old_subscription_identifier",
        "access_method_num",
        "register_date",
        "day_of_week",
        "day_of_month",
        "subscription_status",
        "age",
        "subscriber_tenure",
        "sum_rev_arpu_total_revenue_monthly_last_month",
        "sum_rev_arpu_total_revenue_monthly_last_three_month",
        "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month",
        "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month",
    )

    l5_du_scored = l5_du_scored.join(
        l5_du_scoring_master, ["subscription_identifier"], "left"
    )

    # Only Select Pre-paid Charge type, Convert partition_date to date format
    master_ontop_weekly = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.where(
            "charge_type = 'Prepaid'"
        )
            .withColumn(
            "partition_date_str",
            l0_product_pru_m_ontop_master_for_weekly_full_load["partition_date"].cast(
                StringType()
            ),
        )
            .drop("partition_date")
            .select(
            "price_inc_vat",
            "package_type",
            "promotion_code",
            "package_group",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "package_name_report",
            "data_quota",
            "duration",
            F.to_timestamp("partition_date_str", "yyyyMMdd").alias(
                "partition_date_timestamp"
            ),
        )
            .selectExpr("*", "date(partition_date_timestamp) as partition_date")
            .drop("partition_date_timestamp")
    )

    # Cleansing Master data
    ##
    # Data Quota are transformed from string data to float value of MB, All unlimited are impute with extreme value
    # while null are impute with -1
    ##
    # Data Speed are inferred by it speed, the unit use is Kbps
    ##
    # This weekly data is always updated on Wednesday, However C360 Weekly Data Define start of week by Monday
    # So, we -2 partition_date to get start_of_week for data mapping
    master_ontop_weekly_fixed = master_ontop_weekly.selectExpr(
        "price_inc_vat",
        "package_type",
        "promotion_code",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        """CASE WHEN data_quota LIKE '%GB%' THEN split(data_quota, 'GB')[0] * 1024
                WHEN data_quota LIKE '%MB%' THEN split(data_quota, 'MB')[0]
                WHEN data_quota LIKE '%Hr%' THEN 999999999
                WHEN data_quota LIKE '%D' THEN 999999999
                WHEN data_quota LIKE '%M%' THEN 999999999
                WHEN data_quota LIKE '%Mins%' THEN 999999999
                WHEN data_quota LIKE '%UL%' THEN 999999999
                ELSE -1 END AS data_quota_mb""",
        """CASE WHEN duration LIKE '%D%' THEN split(duration, 'D')[0] 
                                        WHEN duration LIKE '%Hr%' THEN split(duration, 'Hr')[0]/24
                                     ELSE 0 END AS duration """,
        """CASE WHEN mm_data_Speed = 'Entertain' THEN 4096
            WHEN mm_data_Speed = 'Social' THEN 512
            WHEN mm_data_Speed = 'Time' THEN 51200
            WHEN mm_data_Speed = 'Others' THEN 512
            WHEN MM_Data_Speed = '64Kbps'				 THEN    64
            WHEN MM_Data_Speed = '256Kbps'			 THEN   256
      WHEN MM_Data_Speed = '384Kbps'			 THEN   384
      WHEN MM_Data_Speed = '512Kbps'			 THEN   512
      WHEN MM_Data_Speed = '1Mbps'				 THEN  1024
      WHEN MM_Data_Speed = '2Mbps'  			 THEN  2048
      WHEN MM_Data_Speed = '4Mbps'				 THEN  4096
      WHEN MM_Data_Speed = '6Mbps'		    	 THEN  6144
      WHEN MM_Data_Speed = '10Mbps'				 THEN 10240
      WHEN MM_Data_Speed = 'Full speed'			 THEN 51200
      WHEN MM_Data_Speed = 'Full Speed'		     THEN 51200
      WHEN MM_Data_Speed = 'Full Speed - Next G' THEN 51200
      WHEN MM_Data_Speed = '7.2Mbps'             THEN 7372
            ELSE 0
            END as data_speed""",
        "DATE_ADD(partition_date,-2) as start_of_week",
    )
    max_master_date = (
        master_ontop_weekly_fixed.withColumn("G", F.lit(1))
            .groupby("G")
            .agg(F.max("start_of_week"))
            .collect()
    )

    agg_master_ontop = (
        master_ontop_weekly_fixed.where(
            "start_of_week = date('"
            + datetime.datetime.strftime(max_master_date[0][1], "%Y-%m-%d")
            + "')"
        )
            .groupby(
            "package_name_report",
            "package_type",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "data_quota_mb",
            "duration",
            "data_speed",
        )
            .agg(F.count("*").alias("CNT"), F.max("price_inc_vat").alias("price_inc_vat"))
            .drop("CNT")
    )
    agg_master_ontop = agg_master_ontop.selectExpr(
        "package_name_report as offer_package_name_report",
        "package_type as offer_package_type",
        "mm_types as offer_mm_types",
        "mm_data_speed as offer_mm_data_speed",
        "data_quota_mb as offer_data_quota_mb",
        "duration as offer_duration",
        "data_speed as offer_data_speed",
        "price_inc_vat as offer_price_inc_vat",
    )

    atl_campaign_mapping = mapping_for_model_training.where(
        "to_model = 1 AND COUNT_PRODUCT_SELL_IN_CMP = 1 AND Macro_product_Offer_type = 'ATL'"
    )
    btl_campaign_mapping = (
        mapping_for_model_training.where(
            "to_model = 1 AND COUNT_PRODUCT_SELL_IN_CMP = 1 AND Macro_product_Offer_type = 'BTL'"
        )
            .drop("Discount_percent")
            .withColumn(
            "Discount_percent",
            (F.col("highest_price") - F.col("price_inc_vat")) / F.col("highest_price"),
        )
    )

    btl_campaign_mapping = (
        btl_campaign_mapping.where("Discount_percent <= 0.50")
            .drop("Discount_predefine_range")
            .withColumn(
            "Discount_predefine_range",
            F.expr(
                """CASE WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price >= 0.05 AND (highest_price-price_inc_vat)/highest_price <= 0.10 THEN 1
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.10 AND (highest_price-price_inc_vat)/highest_price <= 0.20 THEN 2
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.20 AND (highest_price-price_inc_vat)/highest_price <= 0.30 THEN 3
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.30 AND (highest_price-price_inc_vat)/highest_price <= 0.40 THEN 4
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.40 AND (highest_price-price_inc_vat)/highest_price <= 0.50 THEN 5
     WHEN highest_price != price_inc_vat AND (highest_price-price_inc_vat)/highest_price > 0.50 THEN 6 ELSE 0 END"""
            ),
        )
    )
    atl_campaign_mapping = atl_campaign_mapping.withColumn(
        "rework_macro_product",
        F.concat(F.col("Macro_product"), F.lit("_"), F.col("Macro_product_Offer_type")),
    )
    btl_campaign_mapping = btl_campaign_mapping.withColumn(
        "rework_macro_product",
        F.concat(
            F.col("Macro_product"),
            F.lit("_"),
            F.col("Macro_product_Offer_type"),
            F.lit("_"),
            F.col("Discount_predefine_range"),
        ),
    )
    campaign_mapping = atl_campaign_mapping.union(btl_campaign_mapping)
    model_offer_info = (
        campaign_mapping.selectExpr(
            "macro_product",
            "Package_name as offer_package_name_report",
            "rework_macro_product as model_name",
            "Macro_product_Offer_type as offer_Macro_product_type",
        )
            .groupby("macro_product", "model_name", "offer_Macro_product_type")
            .agg(
            F.count("*").alias("CNT"),
            F.first("offer_package_name_report").alias("offer_package_name_report"),
        )
            .drop("CNT")
            .join(agg_master_ontop, ["offer_package_name_report"], "left")
    )
    l5_du_scored_info = l5_du_scored.join(model_offer_info, ["model_name"], "left")

    max_package_preference_date = (
        l4_data_ontop_package_preference.withColumn("G", F.lit(1))
            .groupby("G")
            .agg(F.max("start_of_week"))
            .collect()
    )
    l5_du_scored_offer_preference = (
        l5_du_scored_info.selectExpr("*", "date(register_date) as register_date_d")
            .drop("register_date")
            .withColumnRenamed("register_date_d", "register_date")
            .join(
            l4_data_ontop_package_preference.drop("access_method_num").where(
                "start_of_week = date('"
                + datetime.datetime.strftime(
                    max_package_preference_date[0][1], "%Y-%m-%d"
                )
                + "')"
            ),
            ["old_subscription_identifier", "register_date"],
            "left",
        )
    )

    l5_du_scored_offer_preference = l5_du_scored_offer_preference.dropDuplicates(
        ["old_subscription_identifier", "model_name"]
    )
    if schema_name == dev_schema_name:
        spark.sql(
            """DROP TABLE IF EXISTS """
            + schema_name
            + """.du_offer_score_with_package_preference"""
        )
        l5_du_scored_offer_preference.createOrReplaceTempView("tmp_tbl")
        spark.sql(
            """CREATE TABLE """
            + schema_name
            + """.du_offer_score_with_package_preference
            AS 
            SELECT * FROM tmp_tbl"""
        )
    else:
        l5_du_scored_offer_preference.write.format("delta").mode("append").partitionBy(
            "scoring_day"
        ).saveAsTable(schema_name + ".du_offer_score_with_package_preference")
    elapsed = format_time(time.time() - t0)
    logging.warning("Node du_join_preference took: {:}".format(elapsed))
    return l5_du_scored_offer_preference
