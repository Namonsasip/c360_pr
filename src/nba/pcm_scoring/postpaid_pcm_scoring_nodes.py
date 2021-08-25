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
)
import time
import datetime

def l5_pcm_postpaid_candidate_with_campaign_info(
    postpaid_pcm_candidate: DataFrame,
    l5_nba_postpaid_campaign_master: DataFrame,
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
) -> DataFrame:

    l5_nba_postpaid_campaign_master = l5_nba_postpaid_campaign_master.withColumn(
        "aux_date_order",
        F.row_number().over(
            Window.partitionBy("child_code").orderBy(
                F.col("month_id").desc()
            )
        ),
    )

    df = postpaid_pcm_candidate.join(
        F.broadcast(l5_nba_postpaid_campaign_master), on="child_code", how="left"
    ).withColumnRenamed("child_code", "campaign_child_code")

    df = df.withColumnRenamed("subscription_identifier", "old_subscription_identifier")

    # Keep only the most recent customer profile to get the mapping
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

    df = df.join(
        df_latest_sub_id_mapping.select(
            "subscription_identifier", "old_subscription_identifier"
        ),
        on=["old_subscription_identifier"],
        how="left",
    )

    return df


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
            key_columns = non_date_join_cols + [table_time_column]
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

        df_master = df_master.join(df_features, on=key_columns, how="left")

    pdf_tables.to_csv(os.path.join("/dbfs/mnt/customer360-blob-output/users/sitticsr", "join_ID_pcm_info.csv"), index=False)


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
    spark = get_spark_session()
    mlflow_path = "/NBA_postpaid"
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
        max_results=300,
        order_by=None,
    )
    all_run_data[model_group_column] = all_run_data["tags.mlflow.runName"]
    mlflow_sdf = spark.createDataFrame(all_run_data.astype(str))
    # df_master = catalog.load("l5_du_scoring_master")
    eligible_model = mlflow_sdf.selectExpr(model_group_column)
    df_master_postpaid_nba = df_master.crossJoin(F.broadcast(eligible_model))


    df_master_postpaid_nba = add_model_group_column(
        df_master_postpaid_nba,
        nba_postpaid_model_group_column_pull_campaign,
        nba_postpaid_model_group_column_push_campaign,
    )

    df_master_postpaid_nba = df_master_postpaid_nba.withColumn(
        "nba_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("candidate_date"),
            F.lit("_"),
            F.col("campaign_child_code"),
        ),
    )
    # Since NBA does not generate a score foe every possible campaign,
    # create a column to mark for which we should score
    # This logic will probably be slightly different in the future sicne
    # NGCM should provide enough info for NBA to know which campaigns
    # it should score and which not
    # df_master = df_master.withColumn(
    #     "to_be_scored",
    #     F.when(
    #         (F.col("campaign_sub_type") == "Non-trigger")
    #         # & (F.substring("campaign_child_code", 1, 4) != "Pull")
    #         & (F.col("model_group") != "NULL")
    #         & (~F.isnull(F.col("model_group"))),
    #         F.lit(1),
    #     ).otherwise(F.lit(0)),
    # )

    # We score only the campaigns that should have a model
    df_master_scored = score_nba_postpaid_models(
        df_master=df_master_postpaid_nba,
        primary_key_columns=["subscription_identifier"],
        model_group_column=model_group_column,
        models_to_score={
            acceptance_model_tag: "propensity",
            arpu_model_tag: "arpu_uplift",
        },
        scoring_chunk_size=scoring_chunk_size,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        missing_model_default_value=0,  # Give NBA score of 0 in case we don't have a model
        mlflow_model_version=mlflow_model_version,
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
