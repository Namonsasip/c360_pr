import logging
import os
from typing import Dict, List

import pandas as pd
import pyspark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import mlflow
from customer360.utilities.spark_util import get_spark_session
from du.models.models_nodes import score_du_models

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
        "old_subscription_identifier",
        "access_method_num",
        "register_date",
        "subscription_identifier",
        "charge_type",
    )
    df_latest_sub_id_mapping = df_latest_sub_id_mapping.where(
        "charge_type = 'Pre-paid'"
    ).drop("charge_type")
    return df_latest_sub_id_mapping


def l5_du_scored(
    df_master: DataFrame,
    l5_average_arpu_untie_lookup: DataFrame,
    model_group_column: str,
    explanatory_features,
    acceptance_model_tag: str,
    mlflow_model_version: str,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    scoring_chunk_size: int = 500000,
    **kwargs,
):
    # Data upsell generate score for every possible upsell campaign
    spark = get_spark_session()
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    # model_group_column = "model_name"
    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='binary' AND params.Able_to_model = 'True' AND params.Version="
        + str(mlflow_model_version),
        run_view_type=1,
        max_results=100,
        order_by=None,
    )
    all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    # df_master = catalog.load("l5_du_scoring_master")
    eligible_model = mlflow_sdf.selectExpr(model_group_column)
    df_master_upsell = df_master.crossJoin(F.broadcast(eligible_model))

    df_master_scored = score_du_models(
        df_master=df_master_upsell,
        primary_key_columns=[
            "subscription_identifier",
            "old_subscription_identifier",
            "access_method_num",
            "register_date",
        ],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        explanatory_features=explanatory_features,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        mlflow_model_version=mlflow_model_version,
        **kwargs,
    )
    # df_master_scored = df_master_scored.join(df_master, on=primary_key_columns, how="left")
    return df_master_scored
