import logging
from typing import Dict, List, Tuple

import pandas as pd
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, TimestampType, DateType

from customer360.utilities.spark_util import get_spark_session
from nba.models.models_nodes import calculate_extra_pai_metrics


def node_l5_nba_customer_profile(
    l3_customer_profile_include_1mo_non_active: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    df_customer_profile = l3_customer_profile_include_1mo_non_active

    df_customer_profile = df_customer_profile.withColumn(
        "charge_type_numeric",
        F.when(F.col("charge_type") == "Pre-paid", 0)
        .when(F.col("charge_type") == "Post-paid", 1)
        .when(F.col("charge_type") == "Hybrid-Post", 2),
    )
    df_customer_profile = df_customer_profile.withColumn(
        "network_type_numeric",
        F.when(F.col("network_type") == "3GPre-paid", 0)
        .when(F.col("network_type") == "3G", 1)
        .when(F.col("network_type") == "FBB", 2)
        .when(F.col("network_type") == "Fixed Line-AWN", 3)
        .when(F.col("network_type") == "Non Mobile-SBN", 4),
    )


    df_customer_profile = df_customer_profile.withColumn(
        "customer_segment_numeric",
        F.when(F.col("customer_segment") == "Classic", 0)
        .when(F.col("customer_segment") == "Standard", 1)
        .when(F.col("customer_segment") == "Gold", 2)
        .when(F.col("customer_segment") == "Platinum", 3)
        .when(F.col("customer_segment") == "Platinum Plus", 4)
        .when(F.col("customer_segment") == "Emerald", 5)
        .when(F.col("customer_segment") == "Prospect Gold", 6)
        .when(F.col("customer_segment") == "Prospect Platinum", 7)
        .when(F.col("customer_segment") == "Prospect Plat Plus", 8)
        .when(F.col("customer_segment") == "Prospect Emerald", 9),
    )

    df_customer_profile = df_customer_profile.withColumn(
        "subscription_status_numeric",
        F.when(F.col("subscription_status") == "Active", 0)
        .when(F.col("subscription_status") == "SA", 1)
        .when(F.col("subscription_status") == "CT", 2)
        .when(F.col("subscription_status") == "SS", 3)
        .when(F.col("subscription_status") == "SD", 4)
        .when(F.col("subscription_status") == "Suspend", 5)
        .when(F.col("subscription_status") == "Suspend - Debt", 6),
    )

    df_customer_profile = df_customer_profile.withColumn(
        "cust_active_this_month_numeric",
        (F.col("cust_active_this_month") == "Y").cast(FloatType()),
    )

    return df_customer_profile

def node_l5_nba_master_table_spine(
    l0_campaign_tracking_contact_list_pre: DataFrame,
    important_campaigns: DataFrame,
    reporting_kpis: DataFrame,
    min_feature_days_lag: int,
) -> DataFrame:

    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)

    common_columns = list(
        set.intersection(
            set(important_campaigns.columns),
            set(l0_campaign_tracking_contact_list_pre.columns),
        )
    )
    if common_columns:
        logging.warning(
            f"There are common columns in l0_campaign_tracking_contact_list_"
            f" pre and important_campaigns list: {', '.join(common_columns)}"
        )
        for common_column in common_columns:
            important_campaigns = important_campaigns.withColumnRenamed(
                common_column, common_column + "_from_important_campaigns"
            )

    df_spine = l0_campaign_tracking_contact_list_pre.join(
        F.broadcast(
            important_campaigns.withColumnRenamed("child_code", "campaign_child_code")
        ),
        on="campaign_child_code",
        how="inner",
    )

    df_spine = df_spine.withColumn(
        "target_response",
        F.when(F.col("response") == "Y", 1)
        .when(F.col("response") == "N", 0)
        .otherwise(None),
    )

    # Add different timeframe columns to join with features
    # Need we assume a lag of min_feature_days_lag to create the features and
    # also need to subtract a month because start_of_month references the first day of
    # the month for which the feature was calculated
    df_spine = df_spine.withColumn(
        "start_of_month",
        F.add_months(
            F.date_trunc(
                "month", F.date_sub(F.col("contact_date"), days=min_feature_days_lag),
            ),
            months=-1,
        ),
    )
    df_spine = df_spine.withColumn("partition_month", F.col("start_of_month"))

    # start_of_week references the first day of the week for which the feature was
    # calculated so we subtract 7 days to not take future data
    df_spine = df_spine.withColumn(
        "start_of_week",
        F.date_sub(
            F.date_trunc(
                "week", F.date_sub(F.col("contact_date"), days=min_feature_days_lag)
            ),
            days=7,
        ),
    )

    # event_partition_date references the day for which the feature was calculated
    df_spine = df_spine.withColumn(
        "event_partition_date",
        F.date_sub(F.col("contact_date"), days=min_feature_days_lag),
    )

    # join_date
    df_spine = df_spine.withColumn("join_date", F.col("contact_date").cast(DateType()))

    # Add ARPU uplift
    df_arpu_30d_before = reporting_kpis.select(
        "subscription_identifier", "join_date", "total_revenue_30_day"
    )
    df_arpu_30d_after = reporting_kpis.select(
        "subscription_identifier",
        F.date_sub(F.col("join_date"), 30).alias("join_date"),
        F.col("total_revenue_30_day").alias("total_revenue_30_day_after"),
    )
    df_arpu_uplift = df_arpu_30d_before.join(
        df_arpu_30d_after, how="inner", on=["subscription_identifier", "join_date"]
    ).select(
        "subscription_identifier",
        F.col("join_date"),
        (F.col("total_revenue_30_day_after") - F.col("total_revenue_30_day")).alias(
            "target_relative_arpu_increase_30d"
        ),
    )

    df_spine = df_spine.join(
        df_arpu_uplift, on=["subscription_identifier", "join_date"], how="left"
    )

    return df_spine


def node_l5_nba_master_table(
    l5_nba_master_table_spine: DataFrame,
    subset_features: Dict[str, List[str]],
    **kwargs: DataFrame,
) -> DataFrame:

    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)

    non_date_join_cols = ["subscription_identifier"]

    df_master = l5_nba_master_table_spine
    possible_key_time_columns = [
        "partition_month",
        "event_partition_date",
        "start_of_month",
        "start_of_week",
    ]

    for table_name, df_features in kwargs.items():

        table_time_column_set = set(df_features.columns).intersection(
            set(possible_key_time_columns)
        )

        if len(table_time_column_set) > 1:
            raise ValueError(
                f"More then one date column found in features table {table_name}, "
                f"columns found are {', '.join(df_features.columns)}"
            )
        elif len(table_time_column_set) <= 0:
            raise ValueError(
                f"Could not find a known time column in features table {table_name}, "
                f"columns found are {', '.join(df_features.columns)}"
            )
        elif len(table_time_column_set) == 1:
            table_time_column = table_time_column_set.pop()

        key_columns = non_date_join_cols + [table_time_column]

        if table_name in subset_features.keys():
            df_features = df_features.select(
                *(key_columns + subset_features[table_name])
            )

        duplicated_columns = [
            col_name
            for col_name in df_master.columns
            if col_name in df_features.columns
        ]
        duplicated_columns = list(set(duplicated_columns) - set(key_columns))
        if duplicated_columns:
            # logging.warning(f"Duplicated column names {', '.join(duplicated_columns)}"
            #                 f" found when joining, they will be dropped from one table")
            raise ValueError(
                f"Duplicated column names {', '.join(duplicated_columns)} found"
                f" when joining features table {table_name} to the master table."
                f"Columns of {table_name} are: {', '.join(df_features.columns)}"
            )

        df_master = df_master.join(df_features, on=key_columns, how="left")

    # Cast decimal type columns cause they don't get properly converted to pandas
    df_master = df_master.select(
        *[
            F.col(column_name).cast(FloatType())
            if column_type.startswith("decimal")
            else F.col(column_name)
            for column_name, column_type in df_master.dtypes
        ],
    )

    return df_master


def node_l5_nba_master_table_only_accepted(
    l5_nba_master_table: DataFrame,
) -> DataFrame:
    return l5_nba_master_table.filter(F.col("response") == "Y")


def node_l5_nba_master_table_chunk_debug_acceptance(
    l5_nba_master_table: DataFrame, child_code: str, sampling_rate: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_chunk = l5_nba_master_table.filter(F.col("campaign_child_code") == child_code)

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        l5_nba_master_table, target_column="target_response", by="campaign_child_code"
    )
    l5_nba_master_table_chunk_debug = (
        df_chunk.filter(~F.isnull(F.col("target_response")))
        .sample(sampling_rate)
        .toPandas()
    )
    return l5_nba_master_table_chunk_debug, pdf_extra_pai_metrics


def node_l5_nba_master_table_chunk_debug_arpu(
    l5_nba_master_table_only_accepted: DataFrame, child_code: str, sampling_rate: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_chunk = l5_nba_master_table_only_accepted.filter(
        F.col("campaign_child_code") == child_code
    )

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        l5_nba_master_table_only_accepted,
        target_column="target_relative_arpu_increase_30d",
        by="campaign_child_code",
    )
    l5_nba_master_table_chunk_debug = (
        df_chunk.filter(
            ~F.isnull(F.col("target_relative_arpu_increase_30d"))
        )
        .sample(sampling_rate)
        .toPandas()
    )
    return l5_nba_master_table_chunk_debug, pdf_extra_pai_metrics
