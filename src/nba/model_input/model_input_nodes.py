import logging
from typing import Dict, List, Tuple, Union

import pandas as pd
import pyspark
from pyspark.sql import DataFrame, Window
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


def node_l5_nba_campaign_master(campaign_history_master_active: DataFrame) -> DataFrame:

    l5_nba_campaign_master = campaign_history_master_active.replace(
        "Cross Sell", value="Cross/ Up sell", subset="campaign_category"
    )

    return l5_nba_campaign_master


def node_l5_nba_master_table_spine(
    l0_campaign_tracking_contact_list_pre: DataFrame,
    l4_revenue_prepaid_daily_features: DataFrame,
    l5_nba_campaign_master: DataFrame,
    prioritized_campaign_child_codes: List[str],
    nba_model_group_column_prioritized: str,
    nba_model_group_column_non_prioritized: str,
    date_min: str,  # YYYY-MM-DD
    date_max: str,  # YYYY-MM-DD
    min_feature_days_lag: int,
) -> DataFrame:
    """

    Args:
        l0_campaign_tracking_contact_list_pre:
        l4_revenue_prepaid_daily_features:
        l5_nba_campaign_master:
        prioritized_campaign_child_codes: List of prioritized campaign child codes
        nba_model_group_column_prioritized: column that contains the group for which
            prioritized cmpaigns will be trained on
        nba_model_group_column_non_prioritized: column that contains the group for which
            non-prioritized cmpaigns will be trained on
        date_min:
        date_max:
        min_feature_days_lag:

    Returns:

    """
    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)

    l0_campaign_tracking_contact_list_pre = l0_campaign_tracking_contact_list_pre.withColumn(
        "contact_date", F.col("contact_date").cast(DateType())
    ).filter(
        F.col("contact_date").between(date_min, date_max)
    )

    common_columns = list(
        set.intersection(
            set(l5_nba_campaign_master.columns),
            set(l0_campaign_tracking_contact_list_pre.columns),
        )
    )
    if common_columns:
        logging.warning(
            f"There are common columns in l0_campaign_tracking_contact_list_pre "
            f"and campaign_history_master_active: {', '.join(common_columns)}"
        )
        for common_column in common_columns:
            l0_campaign_tracking_contact_list_pre = l0_campaign_tracking_contact_list_pre.withColumnRenamed(
                common_column, common_column + "_from_campaign_tracking"
            )

    df_spine = l0_campaign_tracking_contact_list_pre.join(
        F.broadcast(
            l5_nba_campaign_master.withColumnRenamed(
                "child_code", "campaign_child_code",
            )
        ),
        on="campaign_child_code",
        how="left",
    )

    df_spine = df_spine.withColumn(
        "target_response",
        F.when(F.col("response") == "Y", 1)
        .when(F.col("response") == "N", 0)
        .otherwise(None),
    )

    df_spine = add_c60_dates_columns(
        df_spine, date_column="contact_date", min_feature_days_lag=min_feature_days_lag
    )

    # Impute ARPU uplift columns as NA means that subscriber had 0 ARPU
    l4_revenue_prepaid_daily_features = l4_revenue_prepaid_daily_features.fillna(
        0,
        subset=list(
            set(l4_revenue_prepaid_daily_features.columns)
            - set(["subscription_identifier", "event_partition_date"])
        ),
    )

    # Add ARPU uplift
    for n_days, feature_name in [
        (30, "sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
        (7, "sum_rev_arpu_total_net_rev_daily_last_seven_day"),
    ]:
        df_arpu_before = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier", "event_partition_date", feature_name,
        )
        df_arpu_after = l4_revenue_prepaid_daily_features.select(
            "subscription_identifier",
            F.date_sub(F.col("event_partition_date"), n_days).alias(
                "event_partition_date"
            ),
            F.col(feature_name).alias(f"{feature_name}_after"),
        )
        df_arpu_uplift = df_arpu_before.join(
            df_arpu_after,
            how="inner",
            on=["subscription_identifier", "event_partition_date"],
        ).withColumn(
            f"target_relative_arpu_increase_{n_days}d",
            (F.col(f"{feature_name}_after") - F.col(feature_name)),
        )

        # Add the average ARPU on each day for all subscribers in case we want to
        # normalize the ARPU target later
        df_arpu_uplift = (
            df_arpu_uplift.withColumn(
                f"{feature_name}_avg_all_subs",
                F.mean(feature_name).over(Window.partitionBy("event_partition_date")),
            )
            .withColumn(
                f"{feature_name}_after_avg_all_subs",
                F.mean(f"{feature_name}_after").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
            .withColumn(
                f"target_relative_arpu_increase_{n_days}d_avg_all_subs",
                F.mean(f"target_relative_arpu_increase_{n_days}d").over(
                    Window.partitionBy("event_partition_date")
                ),
            )
        )

        df_spine = df_spine.join(
            df_arpu_uplift,
            on=["subscription_identifier", "event_partition_date"],
            how="left",
        )

    # Remove duplicates to make sure the tuple (subscriber, date, child code, is unique)
    # We order by the target to prioritize tracked responses with a positive response
    df_spine = df_spine.withColumn(
        "aux_row_number",
        F.row_number().over(
            Window.partitionBy(
                "subscription_identifier", "contact_date", "campaign_child_code"
            ).orderBy(F.col("target_response").desc_nulls_last())
        ),
    )
    df_spine = df_spine.filter(F.col("aux_row_number") == 1).drop("aux_row_number")

    # Create a primary key for the master table spine
    df_spine = df_spine.withColumn(
        "nba_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("contact_date"),
            F.lit("_"),
            F.col("campaign_child_code"),
        ),
    )

    # Filter master table to model only with relevant campaigns
    df_spine = df_spine.filter(
        (F.col("campaign_type") == "Rule-based")
        & (F.col("campaign_sub_type") == "Non-trigger")
        & (F.substring("campaign_child_code", 1, 4) != "Pull")
    )

    df_spine = add_model_group_column(
        df_spine,
        nba_model_group_column_non_prioritized,
        nba_model_group_column_prioritized,
        prioritized_campaign_child_codes,
    )

    return df_spine


def add_model_group_column(
    df,
    nba_model_group_column_non_prioritized,
    nba_model_group_column_prioritized,
    prioritized_campaign_child_codes,
):

    df = df.withColumn(
        "campaign_prioritized",
        F.when(
            F.col("campaign_child_code").isin(prioritized_campaign_child_codes),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    df = df.withColumn(
        "model_group",
        F.when(
            F.col("campaign_prioritized") == 1,
            F.concat(
                F.lit(f"{nba_model_group_column_prioritized}="),
                F.when(
                    F.isnull(F.col(nba_model_group_column_prioritized)), F.lit("NULL")
                ).otherwise(F.col(nba_model_group_column_prioritized)),
            ),
        ).otherwise(
            F.concat(
                F.lit(f"{nba_model_group_column_non_prioritized}="),
                F.when(
                    F.isnull(F.col(nba_model_group_column_non_prioritized)),
                    F.lit("NULL"),
                ).otherwise(F.col(nba_model_group_column_non_prioritized)),
            )
        ),
    )

    # Fill NAs in group column as that can lead to problems later when converting to
    # pandas and training models
    df = df.fillna("NULL", subset="model_group")

    return df


def add_c60_dates_columns(
    df: DataFrame, date_column: str, min_feature_days_lag: int
) -> DataFrame:

    """
    Adds necessary time columns to join with C360 features
    Args:
        df:
        date_column:
        min_feature_days_lag:

    Returns:

    """
    # Add different timeframe columns to join with features
    # Need we assume a lag of min_feature_days_lag to create the features and
    # also need to subtract a month because start_of_month references the first day of
    # the month for which the feature was calculated
    df = df.withColumn(
        "start_of_month",
        F.add_months(
            F.date_trunc(
                "month", F.date_sub(F.col(date_column), days=min_feature_days_lag),
            ),
            months=-1,
        ),
    )
    df = df.withColumn("partition_month", F.col("start_of_month"))

    # start_of_week references the first day of the week for which the feature was
    # calculated so we subtract 7 days to not take future data
    df = df.withColumn(
        "start_of_week",
        F.date_sub(
            F.date_trunc(
                "week", F.date_sub(F.col(date_column), days=min_feature_days_lag)
            ),
            days=7,
        ),
    )

    # event_partition_date references the day for which the feature was calculated
    df = df.withColumn(
        "event_partition_date",
        F.date_sub(F.col(date_column), days=min_feature_days_lag),
    )

    # Add day of week and month as features
    df = df.withColumn("day_of_week", F.dayofweek(date_column))
    df = df.withColumn("day_of_month", F.dayofmonth(date_column))

    return df


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
                f" when joining features table {table_name} to the master table. "
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
        df_chunk.filter(~F.isnull(F.col("target_relative_arpu_increase_30d")))
        .sample(sampling_rate)
        .toPandas()
    )
    return l5_nba_master_table_chunk_debug, pdf_extra_pai_metrics


def node_prioritized_campaigns_analysis(
    df_master: pyspark.sql.DataFrame,
    prioritized_campaign_child_codes: List[str],
    nba_model_group_column_prioritized: str,
    nba_model_group_column_non_prioritized: str,
    extra_keep_columns: List[str],
) -> pd.DataFrame:

    df_master = df_master.withColumn(
        "campaign_prioritized",
        F.when(
            F.col("campaign_child_code").isin(prioritized_campaign_child_codes),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    df_master = df_master.withColumn(
        "model_group",
        F.when(
            F.col("campaign_prioritized") == 1,
            F.concat(
                F.lit(f"{nba_model_group_column_prioritized}="),
                F.col(nba_model_group_column_prioritized),
            ),
        ).otherwise(
            F.concat(
                F.lit(f"{nba_model_group_column_non_prioritized}="),
                F.col(nba_model_group_column_non_prioritized),
            )
        ),
    )

    pdf_report = (
        df_master.groupby("model_group")
        .agg(
            F.count(F.lit(1)).alias("n_sent_contacts"),
            F.count(F.col("response")).alias("n_tracked_contacts"),
            F.sum("target_response").alias("n_positive_contacts"),
            F.mean((~F.isnull(F.col("response"))).cast(FloatType())).alias(
                "ratio_response_tracked"
            ),
            F.mean((F.col("response") == "Y").cast(FloatType())).alias(
                "acceptance_rate"
            ),
            F.min("contact_date").alias("min_contact_date"),
            F.max("contact_date").alias("max_contact_date"),
            # 7 day ARPU
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_seven_day_avg_all_subs"
            ).alias("avg_arpu_7d_before_all_subcribers"),
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_seven_day_after_avg_all_subs"
            ).alias("avg_arpu_7d_after_all_subcribers"),
            F.mean("target_relative_arpu_increase_7d_avg_all_subs").alias(
                "avg_arpu_7d_increase_all_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "avg_arpu_7d_before_targeted_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day_after").alias(
                "avg_arpu_7d_after_targeted_subcribers"
            ),
            F.mean("target_relative_arpu_increase_7d").alias(
                "avg_arpu_7d_increase_targeted_subcribers"
            ),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_seven_day"),
                )
            ).alias("avg_arpu_7d_before_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_seven_day_after"),
                )
            ).alias("avg_arpu_7d_after_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("target_relative_arpu_increase_7d"),
                )
            ).alias("avg_arpu_7d_increase_positive_responses"),
            # 30 day ARPU
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_thirty_day_avg_all_subs"
            ).alias("avg_arpu_30d_before_all_subcribers"),
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_thirty_day_after_avg_all_subs"
            ).alias("avg_arpu_30d_after_all_subcribers"),
            F.mean("target_relative_arpu_increase_30d_avg_all_subs").alias(
                "avg_arpu_30d_increase_all_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "avg_arpu_30d_before_targeted_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_thirty_day_after").alias(
                "avg_arpu_30d_after_targeted_subcribers"
            ),
            F.mean("target_relative_arpu_increase_30d").alias(
                "avg_arpu_30d_increase_targeted_subcribers"
            ),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
                )
            ).alias("avg_arpu_30d_before_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_thirty_day_after"),
                )
            ).alias("avg_arpu_30d_after_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("target_relative_arpu_increase_30d"),
                )
            ).alias("avg_arpu_30d_increase_positive_responses"),
            *[
                F.first(F.when(F.col("campaign_prioritized") == 1, F.col(x))).alias(x)
                for x in extra_keep_columns
            ],
        )
        .toPandas()
    )

    pdf_report = pdf_report.merge(
        pd.DataFrame({"campaign_child_code": prioritized_campaign_child_codes}),
        on=["campaign_child_code"],
        how="outer",
    ).sort_values(["n_sent_contacts"], ascending=False)

    return pdf_report
