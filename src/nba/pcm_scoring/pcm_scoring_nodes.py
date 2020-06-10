from typing import Dict, List

import pyspark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from customer360.utilities.spark_util import get_spark_session
from nba.models.models_nodes import score_nba_models


def l5_pcm_candidate_with_campaign_info(
    pcm_candidate: DataFrame, l5_nba_campaign_master: DataFrame
) -> DataFrame:
    return pcm_candidate.join(
        F.broadcast(l5_nba_campaign_master), on="child_code", how="left"
    ).withColumnRenamed("child_code", "campaign_child_code")


def join_c360_features_latest_date(
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

        # Keep only the most recent value of each feature
        df_features = df_features.withColumn(
            "aux_date_order",
            F.row_number().over(
                Window.partitionBy(*non_date_join_cols).orderBy(
                    F.col(table_time_column).desc()
                )
            ),
        )
        df_features = df_features.filter(F.col("aux_date_order") == 1).drop(
            "aux_date_order"
        )
        # Rename the time column to keep track of it even though it won't be used for the join
        df_features = df_features.withColumnRenamed(
            table_time_column, f"{table_time_column}_{table_name}"
        )
        table_time_column = f"{table_time_column}_{table_name}"

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

        df_master = df_master.join(df_features, on=non_date_join_cols, how="left")

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


def l5_nba_pcm_candidate_scored(
    df_master: pyspark.sql.DataFrame,
    l5_average_arpu_untie_lookup: pyspark.sql.DataFrame,
    model_group_column: str,
    acceptance_model_tag: str,
    arpu_model_tag: str,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    scoring_chunk_size: int = 500000,
    **kwargs,
):

    # Since NBA does not generate a score foe every possible campaign,
    # create a column to mark for which we should score
    df_master = df_master.withColumn(
        "to_be_scored",
        F.when(
            (F.col("campaign_sub_type") == "Non-trigger")
            & (F.substring("campaign_child_code", 1, 4) != "Pull")
            & (F.col(model_group_column) != "NULL")
            & (~F.isnull(F.col(model_group_column))),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # We score only the campaigns that should have a model
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

    # Add a column with the type of campaign in case NGCM needs
    # to distinguish then for the final prioritization by category
    df_master_scored = df_master_scored.withColumn(
        "priority_category",
        F.when(
            F.col(model_group_column).startswith("model_use_case"),
            "00300_ARPU_increasing_model_based",
        )
        .when(
            F.col(model_group_column).startswith("campaign_child_code"),
            "00400_prioritized_rule_based",
        )
        .when(
            F.col(model_group_column).startswith("campaign_category"),
            "00500_non_prioritized_rule_based",
        ),
    )

    # Join back to the complete master to make sure we keep all rows
    df_master = df_master.join(
        df_master_scored.select(
            "nba_spine_primary_key",
            "priority_category",
            "prediction_acceptance",
            "prediction_arpu",
        ),
        how="left",
        on="nba_spine_primary_key",
    )

    df_master = df_master.withColumn(
        "nba_score", F.col("prediction_acceptance") * F.col("prediction_arpu")
    )

    df_master = df_master.join(l5_average_arpu_untie_lookup, on)

    return df_master
