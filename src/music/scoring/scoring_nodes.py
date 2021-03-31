import logging
import os
from typing import Dict, List

import pandas as pd
import pyspark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
import mlflow
from customer360.utilities.spark_util import get_spark_session
from music.models.models_nodes import score_music_models, score_music_models_existing
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
import time
import datetime


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


def l5_music_lift_scoring_new_sub(
    df_master: DataFrame,
    l5_average_arpu_untie_lookup: DataFrame,
    model_group_column: str,
    explanatory_features,
    acceptance_model_tag: str,
    mlflow_model_version,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    scoring_chunk_size: int = 500000,
    **kwargs,
):
    # patch data
    df_master = df_master.withColumn(
        "music_campaign_type", F.lit("Calling_Melody_New_Acquire")
    )
    # df_master = df_master.withColumnRenamed("event_partition_date_l4_daily_feature_topup_and_volume","event_partition_date")
    # l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.fillna(
    #     0,
    #     subset=list(
    #         set(l4_revenue_prepaid_daily_features.columns)
    #         - set(["subscription_identifier", "event_partition_date"])
    #     ),
    # )
    # # Add ARPU uplift
    # for n_days, feature_name in [
    #     (30, "sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
    #     (7, "sum_rev_arpu_total_net_rev_daily_last_seven_day"),
    # ]:
    #     df_arpu_before = l4_revenue_prepaid_daily_features.select(
    #         "subscription_identifier", "event_partition_date", feature_name,
    #     )
    #     df_arpu_after = l4_revenue_prepaid_daily_features.select(
    #         "subscription_identifier",
    #         F.date_sub(F.col("event_partition_date"), n_days).alias(
    #             "event_partition_date"
    #         ),
    #         F.col(feature_name).alias(f"{feature_name}_after"),
    #     )
    #     df_arpu_uplift = df_arpu_before.join(
    #         df_arpu_after,
    #         how="inner",
    #         on=["subscription_identifier", "event_partition_date"],
    #     ).withColumn(
    #         f"target_relative_arpu_increase_{n_days}d",
    #         (F.col(f"{feature_name}_after") - F.col(feature_name)),
    #     )
    #
    #     # Add the average ARPU on each day for all subscribers in case we want to
    #     # normalize the ARPU target later
    #     df_arpu_uplift = (
    #         df_arpu_uplift.withColumn(
    #             f"{feature_name}_avg_all_subs",
    #             F.mean(feature_name).over(Window.partitionBy("event_partition_date")),
    #         )
    #         .withColumn(
    #             f"{feature_name}_after_avg_all_subs",
    #             F.mean(f"{feature_name}_after").over(
    #                 Window.partitionBy("event_partition_date")
    #             ),
    #         )
    #         .withColumn(
    #             f"target_relative_arpu_increase_{n_days}d_avg_all_subs",
    #             F.mean(f"target_relative_arpu_increase_{n_days}d").over(
    #                 Window.partitionBy("event_partition_date")
    #             ),
    #         )
    #     )
    #
    #     df_master = df_master.join(
    #         df_arpu_uplift,
    #         on=["subscription_identifier", "event_partition_date"],
    #         how="left",
    #     )

    spark = get_spark_session()
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='binary' AND params.Able_to_model = 'True' AND params.Version='"
        + str(mlflow_model_version)
        + "'",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )
    # all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    # mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    # df_master = catalog.load("l5_du_scoring_master")
    # eligible_model = mlflow_sdf.selectExpr(model_group_column)
    # df_master_upsell = df_master.crossJoin(F.broadcast(eligible_model))

    df_master_scored = score_music_models(
        df_master=df_master,
        primary_key_columns=["subscription_identifier",],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            # arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        explanatory_features=explanatory_features,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )
    df_master_scored = df_master_scored.join(
        df_master, ["subscription_identifier", model_group_column], how="left"
    )
    df_master_scored.createOrReplaceTempView("temp_load_view")
    spark.sql("DROP TABLE IF EXISTS prod_musicupsell.l5_calling_melody_new_sub_acquire")
    spark.sql(
        "CREATE TABLE prod_musicupsell.l5_calling_melody_new_sub_acquire USING DELTA AS SELECT * FROM temp_load_view"
    )

    return df_master_scored


def l5_music_lift_scoring_existing(
    df_master: DataFrame,
    l5_average_arpu_untie_lookup: DataFrame,
    model_group_column: str,
    explanatory_features,
    acceptance_model_tag: str,
    mlflow_model_version,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    scoring_chunk_size: int = 500000,
    **kwargs,
):
    # patch data
    df_master = df_master.withColumn(
        "music_campaign_type", F.lit("Calling_Melody_Existing_Upsell")
    )
    spark = get_spark_session()
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='binary' AND params.Able_to_model = 'True' AND params.Version='"
        + str(mlflow_model_version)
        + "'",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )

    df_master_scored = score_music_models_existing(
        df_master=df_master,
        primary_key_columns=["subscription_identifier",],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            # arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        explanatory_features=explanatory_features,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )
    df_master_scored = df_master_scored.join(
        df_master, ["subscription_identifier", model_group_column], how="left"
    )
    df_master_scored.createOrReplaceTempView("temp_load_view")
    spark.sql("DROP TABLE IF EXISTS prod_musicupsell.l5_calling_melody_existing_upsell")
    spark.sql(
        "CREATE TABLE prod_musicupsell.l5_calling_melody_existing_upsell USING DELTA AS SELECT * FROM temp_load_view"
    )

    return df_master_scored
