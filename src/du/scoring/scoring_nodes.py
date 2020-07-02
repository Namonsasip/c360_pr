import logging
import os
from typing import Dict, List

import pandas as pd
import pyspark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

from customer360.utilities.spark_util import get_spark_session

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
    acceptance_model_tag: str,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    scoring_chunk_size: int = 500000,
    **kwargs,
):
    # Data upsell generate score for every possible upsell campaign

    df_master_scored = score_nba_models(
        df_master=df_master.filter(F.col("to_be_scored") == 1),
        primary_key_columns=["nba_spine_primary_key"],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "prediction_acceptance",
            arpu_model_tag: "prediction_arpu",
        },
        scoring_chunk_size=scoring_chunk_size,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        **kwargs,
    )
    return df_master
