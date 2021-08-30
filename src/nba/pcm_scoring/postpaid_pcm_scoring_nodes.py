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
from nba.model_input.postpaid_model_input_nodes import add_model_group_column
from nba.models.postpaid_models_nodes import score_nba_postpaid_models
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
    DateType,
    TimestampType,
)
import time
import datetime

def add_c360_pcm_dates_columns(
    df: DataFrame, date_column: str, min_feature_days_lag: Dict[str, int]
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
                "month",
                F.date_sub(F.col(date_column),days=min_feature_days_lag['postpaid_min_feature_days_lag_monthly'])
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
                "week",
                F.date_sub(F.col(date_column), days=min_feature_days_lag['postpaid_min_feature_days_lag_weekly'])
            ),
            days=7,
        ),
    )

    # event_partition_date references the day for which the feature was calculated
    df = df.withColumn(
        "event_partition_date",
        F.date_sub(F.col(date_column), days=min_feature_days_lag['postpaid_min_feature_days_lag_daily']),
    )

    # Add day of week and month as features
    df = df.withColumn("day_of_week", F.dayofweek(date_column))
    df = df.withColumn("day_of_month", F.dayofmonth(date_column))

    return df


def add_model_group_column_pcm(
    df: pyspark.sql.DataFrame,
    nba_model_group_column_push_campaign: str,
    nba_model_group_column_pull_campaign: str,
):

    # Regression model
    df = df.withColumn(
        'model_group_for_regression',
        F.when(
            F.col('camp_priority_group').isin('2', '4', '5'),
            F.when(
                # Push campaign
                F.col('push_pull_camp').contains('Post push'),
                F.concat(
                    F.lit(f"{nba_model_group_column_push_campaign}="),
                    F.when(
                        F.isnull(F.col(nba_model_group_column_push_campaign)),
                        F.lit("NULL"),
                    ).otherwise(F.col(nba_model_group_column_push_campaign)),
                    F.lit('_'),
                    F.col('scenario')
                )
            ).when(
                # Pull campaign
                F.col('push_pull_camp').contains('Post pull'),
                F.concat(
                    F.lit(f"{nba_model_group_column_pull_campaign}="),
                    F.when(
                        F.isnull(F.col(nba_model_group_column_pull_campaign)),
                        F.lit("NULL"),
                    ).otherwise(F.col(nba_model_group_column_pull_campaign)),
                    F.lit('_'),
                    F.col('scenario')
                )
            ).otherwise(
                F.lit('NULL')
            )
        ).otherwise(
            F.lit('NULL')
        )
    )

    # Binary model
    df = df.withColumn(
        'model_group_for_binary',
        F.when(
            F.col('camp_priority_group').isin('1', '2', '3', '4', '5'),
            F.when(
                # Push campaign
                F.col('push_pull_camp').contains('Post push'),
                F.concat(
                    F.lit(f"{nba_model_group_column_push_campaign}="),
                    F.when(
                        F.isnull(F.col(nba_model_group_column_push_campaign)),
                        F.lit("NULL"),
                    ).otherwise(F.col(nba_model_group_column_push_campaign)),
                )
            ).when(
                # Pull campaign
                F.col('push_pull_camp').contains('Post pull'),
                F.concat(
                    F.lit(f"{nba_model_group_column_pull_campaign}="),
                    F.when(
                        F.isnull(F.col(nba_model_group_column_pull_campaign)),
                        F.lit("NULL"),
                    ).otherwise(F.col(nba_model_group_column_pull_campaign)),
                )
            ).otherwise(
                F.lit('NULL')
            )
        ).otherwise(
            F.lit('NULL')
        )
    )

    df = df.withColumn(
        'aux_row_number',
        F.row_number().over(
            Window.partitionBy(
                'subscription_identifier',
                'contact_date',
                'campaign_child_code'
            ).orderBy(F.col("target_response").desc_nulls_last())
        )
    )
    df = df.withColumn(
        'model_group_for_binary',
        F.when(
            F.col('aux_row_number') == 1,
            F.col('model_group_for_binary')
        ).otherwise(
            F.lit('NULL')
        )
    ).drop('aux_row_number')

    # Fill NAs in group column as that can lead to problems later when converting to
    # pandas and training models
    df = df.fillna("NULL", subset=["model_group_for_binary", "model_group_for_regression"])

    return df


def l5_pcm_postpaid_candidate_with_campaign_info(
    l5_nba_postpaid_campaign_master: DataFrame,
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    l4_revenue_postpaid_average_by_bill_cycle: DataFrame,
    l4_campaign_postpaid_prepaid_features,
    l0_campaign_tracking_contact_list_post,
    postpaid_min_feature_days_lag: Dict[str, int],
    nba_model_group_column_push_campaign: str,
    nba_model_group_column_pull_campaign: str,
    date_max: str,  # YYYY-MM-DD
) -> DataFrame:

    # df = postpaid_pcm_candidate.join(
    #     F.broadcast(l5_nba_postpaid_campaign_master), on="child_code", how="left"
    # ).withColumnRenamed("child_code", "campaign_child_code")
    #
    # df = df.withColumnRenamed("subscription_identifier", "old_subscription_identifier")

    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)


    l0_campaign_tracking_contact_list_post = l0_campaign_tracking_contact_list_post.withColumn(
        "contact_date", F.col("contact_date").cast(DateType())
    ).filter(
        F.col("contact_date") > date_max)

    common_columns = list(
        set.intersection(
            set(l5_nba_postpaid_campaign_master.columns),
            set(l0_campaign_tracking_contact_list_post.columns),
        )
    )
    if common_columns:
        logging.warning(
            f"There are common columns in l0_campaign_tracking_contact_list_post "
            f"and campaign_history_master_active: {', '.join(common_columns)}"
        )
        for common_column in common_columns:
            l0_campaign_tracking_contact_list_post = l0_campaign_tracking_contact_list_post.withColumnRenamed(
                common_column, common_column + "_from_campaign_tracking"
            )

    df_spine = l0_campaign_tracking_contact_list_post.join(
        F.broadcast(
            l5_nba_postpaid_campaign_master.withColumnRenamed(
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

    df_spine = add_c360_pcm_dates_columns(
        df_spine, date_column="contact_date", min_feature_days_lag=postpaid_min_feature_days_lag
    )  # TODO change min_feature_days_lag to value that suitable for postpaid (cosider data flow of NBO) DONE

    # subscription_identifier is different in L0 and all other C360 levels, so we need to add
    # both of them to the spine, for which we use l1 customer profile as an auxiliary table
    df_spine = df_spine.withColumnRenamed(
        "subscription_identifier", "old_subscription_identifier"
    ).withColumnRenamed("mobile_no", "access_method_num")
    df_spine = df_spine.join(
        l1_customer_profile_union_daily_feature_full_load.select(
            "subscription_identifier", "access_method_num", "charge_type", "event_partition_date",
        ),
        on=["access_method_num", "event_partition_date"],
        how="left",
    )

    # Post-paid customers
    df_spine = df_spine.filter(F.col('charge_type') == 'Post-paid').drop('charge_type')

    df_spine = df_spine.join(
        l4_campaign_postpaid_prepaid_features.select(
            "subscription_identifier",
            "sum_campaign_overall_count_sum_weekly_last_week",
            "sum_campaign_overall_count_sum_weekly_last_four_week",
            "sum_campaign_overall_count_sum_weekly_last_twelve_week",
            "sum_campaign_total_by_sms_sum_weekly_last_week",
            "sum_campaign_total_by_sms_sum_weekly_last_twelve_week",
            "sum_campaign_total_others_by_sms_sum_weekly_last_week",
            "",
        ),
        on=["subscription_identifier", "start_of_week"],
        how="left",
    )
    # # Create key join for bill cycle data flow
    # invoice_summary = l4_revenue_postpaid_average_by_bill_cycle.select(
    #     'invoice_date',
    #     'subscription_identifier',
    # ).withColumn(
    #     'day_of_invoice',
    #     F.dayofmonth(F.col('invoice_date'))
    # ).withColumn(
    #     'start_of_month_invoice_summary',
    #     F.date_trunc('month', F.col('invoice_date'))
    # ).drop('invoice_date')
    #
    # df_spine = df_spine.withColumn(
    #     'start_of_month_invoice_summary',
    #     F.add_months(
    #         F.date_trunc('month', F.col('contact_date')),
    #         months=-1
    #     )
    # )
    #
    # df_spine = df_spine.join(
    #     invoice_summary,
    #     on=['subscription_identifier', 'start_of_month_invoice_summary']
    # )
    #
    # def change_day_(date, day):
    #     return date.replace(day=day)
    #
    # change_day = F.udf(change_day_, TimestampType())
    #
    # df_spine = df_spine.selectExpr(
    #     "*",
    #     "date_sub(contact_date, day_of_invoice) AS contact_date_sub_inv_date"
    # )
    #
    # df_spine = df_spine.withColumn(
    #     'contact_invoice_date',
    #     change_day(
    #         F.date_trunc(
    #             'month',
    #             F.date_sub(
    #                 F.col('contact_date_sub_inv_date'),
    #                 4
    #             )
    #         ),
    #         F.col('day_of_invoice')
    #     )
    # )
    #
    # # Drop duplicate columns
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.drop(
    #     "access_method_num",
    #     "register_date"
    # )
    #
    # # Impute ARPU uplift columns as NA means that subscriber had 0 ARPU
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.fillna(
    #     0,
    #     subset=list(
    #         set(l4_revenue_postpaid_average_by_bill_cycle.columns)
    #         - {"subscription_identifier", "invoice_date", "bill_cycle"}
    #     ),
    # )
    #
    # # Add ARPU uplift by scenario
    #
    # # Change mainpromo scenario
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "avg_revn_mainpromo_last_three_months_after",
    #     F.lead(F.col("avg_revn_mainpromo_last_three_months"), count=5).over(
    #         Window.partitionBy("subscription_identifier").orderBy(F.asc("invoice_date"))
    #     )
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "target_relative_arpu_increase_change_mainpromo",
    #     F.col("avg_revn_mainpromo_last_three_months_after") - (
    #             F.col("avg_revn_mainpromo_last_three_months") +
    #             F.col("avg_revn_ppu_last_three_months") +
    #             F.col("avg_revn_ontop_voice_and_data_last_three_months")
    #     )
    # )
    #
    # # Buy ontop voice and data scenario
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "target_relative_arpu_increase_buy_ontop_voice_and_data",
    #     (F.col("revn_mainpromo") + F.col("revn_ontop_voice_and_data")) - (
    #             F.col("avg_revn_mainpromo_last_three_months") +
    #             F.col("avg_revn_ppu_last_three_months") +
    #             F.col("avg_revn_ontop_voice_and_data_last_three_months")
    #     )
    # )
    #
    # # Buy ontop contents scenario
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "avg_revn_ontop_others_last_five_months_after",
    #     F.lead(F.col("avg_revn_ontop_others_last_five_months"), count=5).over(
    #         Window.partitionBy("subscription_identifier").orderBy(F.asc("invoice_date"))
    #     )
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "target_relative_arpu_increase_buy_ontop_contents",
    #     F.col("avg_revn_ontop_others_last_five_months_after") - (
    #         F.col("avg_revn_ontop_others_last_three_months")
    #     )
    # )
    #
    # # Get information scenario
    # # Add the average ARPU on each day for all subscribers in case we want to
    # # normalize the ARPU target later
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "avg_revn_tot_last_three_months_avg_all_subs",
    #     F.mean("avg_revn_tot_last_three_months").over(Window.partitionBy("invoice_date")),
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "avg_revn_tot_last_three_months_after",
    #     F.lead(F.col("avg_revn_tot_last_three_months"), count=4).over(
    #         Window.partitionBy("subscription_identifier").orderBy(F.asc("invoice_date"))
    #     )
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "avg_revn_tot_last_three_months_after_avg_all_subs",
    #     F.mean("avg_revn_tot_last_three_months_after").over(Window.partitionBy("invoice_date")),
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     "target_relative_arpu_increase_get_information",
    #     F.col("avg_revn_tot_last_three_months_after_avg_all_subs") - (
    #         F.col("avg_revn_tot_last_three_months_avg_all_subs")
    #     )
    # )
    #
    # l4_revenue_postpaid_average_by_bill_cycle = l4_revenue_postpaid_average_by_bill_cycle.withColumn(
    #     'contact_invoice_date',
    #     F.col('invoice_date')
    # )
    #
    # df_spine = df_spine.join(
    #     l4_revenue_postpaid_average_by_bill_cycle,
    #     on=["subscription_identifier", "contact_invoice_date"],
    #     how="left",
    # )
    #
    # # Remove duplicates to make sure the tuple (subscriber, date, child code, is unique)
    # # We order by the target to prioritize tracked responses with a positive response
    # df_spine = df_spine.withColumn(
    #     "aux_row_number",
    #     F.row_number().over(
    #         Window.partitionBy(
    #             "subscription_identifier", "contact_date", "campaign_child_code"
    #         ).orderBy(F.col("target_response").desc_nulls_last())
    #     ),
    # )
    # df_spine = df_spine.filter(F.col("aux_row_number") == 1).drop("aux_row_number")
    #
    # # df_spine = df_spine.dropDuplicates(subset=["subscription_identifier", "contact_date", "campaign_child_code"])
    #
    # scenario_dict = {'nba_main': 'target_relative_arpu_increase_change_mainpromo',
    #                  'nba_ontop': 'target_relative_arpu_increase_buy_ontop_voice_and_data',
    #                  'nba_vas_ontop': 'target_relative_arpu_increase_buy_ontop_contents',
    #                  'nba_information': 'target_relative_arpu_increase_get_information'}
    #
    # for scenario_keys, scenario_value in scenario_dict.items():
    #     df_scenario = df_spine.filter(F.col(scenario_keys) == 'Y')
    #     df_scenario = df_scenario.withColumn(
    #         'target_relative_arpu_increase', F.col(scenario_value)
    #     ).withColumn(
    #         'scenario',
    #         F.lit(scenario_keys)
    #     )
    #     if scenario_keys == 'nba_main':
    #         df_spine_done = df_scenario
    #     else:
    #         df_spine_done = df_spine_done.union(df_scenario)
    #
    # df_spine_done = df_spine_done.drop(
    #     'target_relative_arpu_increase_change_mainpromo',
    #     'target_relative_arpu_increase_buy_ontop_voice_and_data',
    #     'target_relative_arpu_increase_buy_ontop_contents',
    #     'target_relative_arpu_increase_get_information',
    #     'nba_main',
    #     'nba_ontop',
    #     'nba_vas_ontop',
    #     'nba_information'
    # )
    #
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
    #
    # df_spine_done = add_model_group_column_pcm(
    #     df_spine_done,
    #     nba_model_group_column_push_campaign,
    #     nba_model_group_column_pull_campaign,
    # )

    return df_spine


def join_c360_postpaid_features_latest_date(
    df_spine: DataFrame, subset_features: Dict[str, List[str]], **kwargs: DataFrame,
):
    """
        Left-joins C360 features to a spine adding ONLY THE MOST RECENT value for each
        feature. THIS FUNCTION SHOULD ONLY BE USED IN PRODUCTION WHERE THE MOST RECENT
        VALUE OF A FEATURE IS REQUIRED BUT NOT FOR BACKTESTING OR ANY KIND OF EX-POST
        ANALYSIS.
        This is a very computationally expensive operation
        and can take a long time and require a large cluster (e.g. 40 Standard_E16s_v3).
        Args:
            df_spine:
            subset_features: Dictionary where keys are table names and values are a list of
                strings with the features name to keep. No need to specify key (subscriber
                and date) columns in here as they will be automatically selected
            **kwargs: tables to join, key is table name and value is the DataFrame

        Returns: the master tables with all features

        """

    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)

    non_date_join_cols = ["subscription_identifier"]

    df_master = df_spine
    possible_key_time_columns = [
        "partition_month",
        "event_partition_date",
        "start_of_month",
        "start_of_week",
        "contact_invoice_date",
    ]

    pdf_tables = pd.DataFrame()

    for table_name, df_features in kwargs.items():

        if table_name == "l0_revenue_nbo_postpaid_input_data":
            df_features = df_features.withColumnRenamed(
                "vat_date",
                "contact_invoice_date"
            ).withColumnRenamed(
                "crm_subscription_id",
                "subscription_identifier"
            )

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

        # Rename the time column to keep track of it even though it won't be used for the join
        df_features = df_features.withColumnRenamed(
            table_time_column, f"{table_time_column}_{table_name}"
        )

        table_time_column = f"{table_time_column}_{table_name}"
        # Temporary trick to join while C360 features are not migrated to
        # the new subscription_identifier
        subs_sample = (
            df_features.select("subscription_identifier")
            .sample(1e-3)
            .limit(100)
            .toPandas()
        )
        max_sub_len = max(subs_sample["subscription_identifier"].apply(len))
        is_old_id = max_sub_len < 30
        longest_id = list(
            subs_sample["subscription_identifier"][
                subs_sample["subscription_identifier"].apply(len) == max_sub_len
            ]
        )[0]

        if is_old_id:
            logging.warning(
                f"OLD!!!! Table {table_name} has old ID: largest is: {longest_id}. Len is: {max_sub_len}"
            )
            non_date_join_cols = ["old_subscription_identifier"]
            key_columns = ["old_subscription_identifier"] + [table_time_column]
            df_features = df_features.withColumnRenamed(
                "subscription_identifier", "old_subscription_identifier"
            )
            pdf_tables = pd.concat(
                [
                    pdf_tables,
                    pd.DataFrame(
                        {
                            "table": [table_name],
                            "type": ["old"],
                            "longest_id": [longest_id],
                        }
                    ),
                ]
            )
        else:
            logging.warning(
                f"NEW!!!! Table {table_name} has new ID: largest is: {longest_id}. Len is: {max_sub_len}"
            )
            non_date_join_cols = ["subscription_identifier"]
            key_columns = non_date_join_cols + [table_time_column]
            pdf_tables = pd.concat(
                [
                    pdf_tables,
                    pd.DataFrame(
                        {
                            "table": [table_name],
                            "type": ["new"],
                            "longest_id": [longest_id],
                        }
                    ),
                ]
            )

        # Keep only the most recent value of each feature

        max_date = df_features.agg(F.max(table_time_column)).collect()[0][0]
        df_features = df_features.filter(f"{table_time_column} == '{max_date}'")

        # df_features = df_features.withColumn(
        #     "aux_date_order",
        #     F.row_number().over(
        #         Window.partitionBy(*non_date_join_cols).orderBy(
        #             F.col(table_time_column).desc()
        #         )
        #     ),
        # )
        # df_features = df_features.filter(F.col("aux_date_order") == 1).drop(
        #     "aux_date_order"
        # )

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

        df_master = df_master.join(df_features, on=non_date_join_cols, how="left")

    pdf_tables.to_csv(os.path.join("/dbfs/mnt/customer360-blob-output/users/sitticsr", "join_ID_pcm_info2.csv"), index=False)

    # df_master = df_master.dropDuplicates(subset=["subscription_identifier", "contact_date", "campaign_child_code"])

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


def l5_nba_pcm_postpaid_candidate_scored(
    df_master: pyspark.sql.DataFrame,
    l5_postpaid_average_arpu_untie_lookup: pyspark.sql.DataFrame,
    model_group_column,
    model_group_binary,
    model_group_regression,
    # prioritized_campaign_child_codes: List[str],
    nba_postpaid_model_group_column_push_campaign: str,
    nba_postpaid_model_group_column_pull_campaign: str,
    # nba_model_use_cases_child_codes: Dict[str, List[str]],
    acceptance_model_tag: str,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    mlflow_model_version,
    explanatory_features: List[str],
    scoring_chunk_size: int = 500000,
    **kwargs,
):
    # Add day of week and month as features
    # df_master = df_master.withColumn("day_of_week", F.dayofweek("candidate_date"))
    # df_master = df_master.withColumn("day_of_month", F.dayofmonth("candidate_date"))

    # Data upsell generate score for every possible upsell campaign
    # spark = get_spark_session()
    mlflow_path = "/NBA_postpaid"

    # filter model_group that not NULL for predict dcore
    df_master_postpaid_nba = df_master.filter(df_master.model_group_for_binary != "NULL")

    # We score only the campaigns that should have a model
    # Predict propensity score : Binary Model
    df_master_scored = score_nba_postpaid_models(
        df_master=df_master_postpaid_nba,
        primary_key_columns=["subscription_identifier"],
        model_group_column=model_group_binary,
        models_to_score={
            acceptance_model_tag: "prediction_acceptance",
            # arpu_model_tag: "prediction_arpu",
        },
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        mlflow_model_version=mlflow_model_version,
        mlflow_path=mlflow_path,
        explanatory_features=explanatory_features,
        missing_model_default_value=0,  # Give NBA score of 0 in case we don't have a model
        scoring_chunk_size=scoring_chunk_size,
        **kwargs,
    )

    # Predict ARPU : Regression Model
    df_master_scored = score_nba_postpaid_models(
        df_master=df_master_scored,
        primary_key_columns=["subscription_identifier"],
        model_group_column=model_group_regression,
        models_to_score={
            # acceptance_model_tag: "prediction_acceptance",
            arpu_model_tag: "prediction_arpu",
        },
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        mlflow_model_version=mlflow_model_version,
        mlflow_path=mlflow_path,
        explanatory_features=explanatory_features,
        missing_model_default_value=0,  # Give NBA score of 0 in case we don't have a model
        scoring_chunk_size=scoring_chunk_size,
        **kwargs,
    )

    # Add a column with the type of campaign in case NGCM needs
    # to distinguish then for the final prioritization by category
    # In this case we are addding the column here but in the future
    # NGCM must provide this info directly in the input file
    df_master_scored = df_master_scored.withColumn(
        "priority_category",
        F.when(
            F.col("camp_priority_group") == "1",
            "00100_priority_1_churn",
        )
        .when(
            F.col("camp_priority_group") == "2",
            "00200_priority_2_information_churn",
        )
        .when(
            F.col("camp_priority_group") == "3",
            "00300_priority_3_ard",
        )
        .when(
            F.col("camp_priority_group") == "4",
            "00400_priority_4_nba",
        )
        .when(
            F.col("camp_priority_group") == "5",
            "00500_priority_5_information",
        )
    )

    # Join back to the complete master to make sure we keep all rows
    # df_master = df_master.join(
    #     df_master_scored.select(
    #         "nba_spine_primary_key",
    #         "priority_category",
    #         "prediction_acceptance",
    #         "prediction_arpu",
    #     ),
    #     how="left",
    #     on="nba_spine_primary_key",
    # )

    # Calculate NBA score
    df_master = df_master_scored.withColumn(
        "nba_score", F.col("prediction_acceptance") * F.col("prediction_arpu")
    )

    # For Information campaigns NBA score will be 0
    df_master = df_master.withColumn(
        "nba_score",
        F.when(F.col("push_pull_camp") == "Information", F.lit(0)).otherwise(
            F.col("nba_score")
        ),
    )

    # In case the NGCM score is the same for several campaigns (i.e.
    # two non-prioritized campaigns from the same category), we use the
    # average ARPU increase among all targeted subscribers as the KPI to decide
    # which campaign to send
    df_master = df_master.join(l5_postpaid_average_arpu_untie_lookup, on="campaign_child_code")

    # NBA score is a decimal number, but NGCM only allows an integer between 1 and 10000
    # so a rescaling is required in order to make the score compatible with NGCM
    # The rescaling is done by adding a group baseline and a NBA model component:
    #
    # The group baseline depends on the priority group in order to ensure that
    # the priority of groups is preserved, so each group is assigned a range
    # in the 1-10000 interval:
    #  - Churn model based: 9000
    #  - ARD model based: 8000
    #  - ARPU increasing model based: 5500 to 7500 (baseline is 6500)
    #  - Prioritized rule based: 3000 to 5000 (baseline is 4000)
    #  - Non-prioritized based: 500 to 2500 (baseline is 1500)

    df_master = df_master.withColumn(
        "baseline_group_ngcm_score",
        F.when(
            F.col("priority_category") == "00100_priority_1_churn",
            F.lit(int(9000))
        )
            # Churn prevention will have the highest priority among non-prioritized rule-based
            .when(
            F.col("priority_category") == "00200_priority_2_information_churn",
            F.lit(int(8000)))

            .when(
            F.col("priority_category") == "00300_priority_3_ard",
            F.lit(int(6500)),
        )
            .when(
            F.col("priority_category") == "00400_priority_4_nba",
            F.lit(int(4500)),
        )
            .when(
            F.col("priority_category") == "00500_non_prioritized_rule_based",
            F.lit(int(3000)),
        )
            .when(
            F.col("priority_category") == "00500_priority_5_information",
            F.lit(int(1000)),
        ),
    )

    # Then, the NBA model component is added to the baseline to prioritize campaigns
    # within the same group. There are 2 options here
    use_sorting_for_ncgm_score = True
    if not use_sorting_for_ncgm_score:
        # OPTION 1:
        # The NBA component is calculated as the integer part of
        # the NBA score multiplied by a certain multiplier (10 currently). In the
        # unlikely event that the NBA model component would be high enough to go
        # out of the range specified for the group, it will be clipped to make sure
        # this does not happen
        nba_to_ngcm_score_multiplier = 10
        df_master = df_master.withColumn(
            "nba_rescaled_model_component",
            F.col("nba_score") * nba_to_ngcm_score_multiplier,
        )

    else:
        # OPTION 2:
        # The NBA component is just an integer with the order the campaign has
        # according to the priority. Worst campaign is 1, next is 2, etc
        df_master = df_master.withColumn(
            "nba_rescaled_model_component",
            F.row_number().over(
                Window.partitionBy(
                    "subscription_identifier", "priority_category"
                ).orderBy("nba_score", "average_arpu_increase_30d")
            ),
        )

    # We clip the nba model component so that we never exceed the priority group range
    df_master = df_master.withColumn(
        "nba_rescaled_model_component",
        F.least(F.col("nba_rescaled_model_component"), F.lit(900)),
    )
    df_master = df_master.withColumn(
        "nba_rescaled_model_component",
        F.greatest(F.col("nba_rescaled_model_component"), F.lit(-900)),
    )
    df_master.withColumn(
        "ngcm_score",
        (
            F.col("baseline_group_ngcm_score") + F.col("nba_rescaled_model_component")
        ).cast(IntegerType()),
    )

    # For campaigns that are not to be scored just
    # return the same score that was given
    # We cannot replicate this currently because PCM candidate does
    # not have the score, but NGCM will give it
    # TODO change this for NGCM
    # df_master.withColumn(
    #     "ngcm_score",
    #     F.when(F.col("to_be_scored") == 1, F.col("ngcm_score")).otherwise(F.col("NGCM_initial score"))
    # )
    return df_master
