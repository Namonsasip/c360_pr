import logging
from typing import Dict, List

import pandas as pd
import pyspark
from pyspark.sql import Window, DataFrame
from pyspark.sql import functions as F

from customer360.utilities.spark_util import get_spark_session
from nba.model_input.model_input_nodes import (
    add_c360_dates_columns,
    add_model_group_column,
)

from nba.models.models_nodes import score_nba_models

# TODO delete
DATES_LIST = [
    "2020-01-04",
    "2020-01-09",
    "2020-01-14",
    "2020-01-19",
    "2020-01-24",
    "2020-01-29",
]


def calculate_impact_scenario(
    df: pyspark.sql.DataFrame,
    partition_columns: List[str],
    model_group_columns: List[str],
    acceptance_column_str: str,
    arpu_column_str: str,
    calculate_optimal_allocation: bool,
) -> pd.DataFrame:

    df = df.withColumn(
        "value_estimation", F.col(acceptance_column_str) * F.col(arpu_column_str),
    )

    if calculate_optimal_allocation:
        df = df.withColumn(
            "aux_priority",
            F.row_number().over(
                Window.partitionBy(*partition_columns).orderBy(
                    F.col("value_estimation").desc()
                )
            ),
        )
        df = df.filter(F.col("aux_priority") == 1)

    impact_kpis_aggregations = [
        F.count(F.lit(1)).alias("n_contacts_sent"),
        F.mean(F.col(acceptance_column_str)).alias("avg_probability_acceptance"),
        F.sum(F.col(acceptance_column_str)).alias("n_contacts_accepted"),
        F.mean(F.col(arpu_column_str)).alias("average_arpu_if_accepted"),
        F.sum(F.col("value_estimation")).alias("total_expected_revenue"),
        F.mean(F.col("value_estimation")).alias(
            "expected_revenue_per_contacted_subscriber"
        ),
    ]

    pdf_impact_kpis_total = df.select(
        *(
            [F.lit("_TOTAL").alias(x) for x in model_group_columns]
            + impact_kpis_aggregations
        )
    ).toPandas()
    pdf_impact_kpis_by_group = (
        df.groupby(*model_group_columns).agg(*impact_kpis_aggregations).toPandas()
    )

    pdf_impact_kpis = pd.concat(
        [pdf_impact_kpis_total, pdf_impact_kpis_by_group]
    ).sort_values(model_group_columns)

    return pdf_impact_kpis


def l5_nba_backtesting_master_expanded_scored(
    df_master: pyspark.sql.DataFrame,
    model_groups_to_score: List[str],
    model_group_column: str,
    models_to_score: Dict[str, str],
    scoring_chunk_size: int,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    **kwargs,
):

    # TODO remove
    # df_master = df_master.filter(
    #     F.col("contact_date").cast(DateType()).isin(DATES_LIST)
    # )
    df_master = df_master.sample(0.02)

    # Drop NAs in the target as we can only run the backtest for tracked campaigns
    df_master = df_master.dropna(
        subset=[
            "target_response",
            "target_relative_arpu_increase_30d",
            "target_relative_arpu_increase_7d",
        ]
    )

    spark = get_spark_session()
    df_master = df_master.filter(F.col(model_group_column).isin(model_groups_to_score))
    df_master = df_master.withColumnRenamed(
        model_group_column, f"original_{model_group_column}"
    )
    df_model_groups = spark.createDataFrame(
        pd.DataFrame({model_group_column: model_groups_to_score})
    )

    df_master_expanded = df_master.crossJoin(F.broadcast(df_model_groups))
    df_master_expanded_with_scores = score_nba_models(
        df_master=df_master_expanded,
        primary_key_columns=["nba_spine_primary_key", "model_group"],
        model_group_column=model_group_column,
        models_to_score=models_to_score,
        scoring_chunk_size=scoring_chunk_size,
        pai_runs_uri=pai_runs_uri,
        pai_artifacts_uri=pai_artifacts_uri,
        **kwargs,
    )

    return df_master_expanded_with_scores


def l5_nba_backtesting_master_scored(
    l5_nba_backtesting_master_expanded_scored: pyspark.sql.DataFrame,
    model_group_column: str,
) -> pyspark.sql.DataFrame:
    df_master_with_scores = l5_nba_backtesting_master_expanded_scored.filter(
        F.col(f"original_{model_group_column}") == F.col(model_group_column)
    )
    return df_master_with_scores


def l5_nba_backtesting_master_expanded_queue_distribution_scored(
    l5_nba_backtesting_master_expanded_scored: pyspark.sql.DataFrame,
    queue_distribution: Dict[int, float],
    partition_columns: List[str],
    model_group_column: str,
) -> pyspark.sql.DataFrame:

    df = l5_nba_backtesting_master_expanded_scored.withColumn(
        "random_queue_priority",
        F.when(
            (F.col(f"original_{model_group_column}") == F.col(model_group_column)),
            F.lit(99),
        ).otherwise(F.rand()),
    )
    df = df.withColumn(
        "aux_random_by_group",
        F.first(F.rand()).over(Window.partitionBy(*partition_columns)),
    )
    df = df.withColumn("queue_length", F.lit(1))
    cum_probability = 0
    for queue_length, probability in queue_distribution.items():
        df = df.withColumn(
            "queue_length",
            F.when(
                F.col("aux_random_by_group").between(
                    cum_probability, cum_probability + probability
                ),
                F.lit(queue_length),
            ).otherwise(F.col("queue_length")),
        )
        cum_probability += probability

    df = df.withColumn(
        "position_in_queue",
        F.row_number().over(
            Window.partitionBy(*partition_columns).orderBy(
                F.col("random_queue_priority")
            )
        ),
    )

    df = df.filter(
        (F.col(f"original_{model_group_column}") == F.col(model_group_column))
        | (F.col("position_in_queue") <= (F.col("queue_length") - 1))
    )

    df = df.drop(
        "random_queue_priority",
        "aux_random_by_group",
        "queue_length",
        "position_in_queue",
    )

    return df


def backtest_campaign_contacts(
    df_master_with_scores: pyspark.sql.DataFrame,
    df_master_expanded_with_scores: pyspark.sql.DataFrame,
    model_group_column: str,
) -> pyspark.sql.DataFrame:

    # # TODO delete
    # df_master_expanded_with_scores = df_master_expanded_with_scores.join(
    #     df_master_with_scores.select("subscription_identifier", F.col("contact_date").cast(DateType()).alias("candidate_date")).distinct(),
    #     on = ["subscription_identifier", "candidate_date"]
    # )
    # df_master_with_scores = df_master_with_scores.withColumn("contact_date", F.col("contact_date").cast(DateType())).join(
    #     df_master_expanded_with_scores.select("subscription_identifier", F.col("candidate_date").alias("contact_date")).distinct(),
    #     on = ["subscription_identifier", "contact_date"]
    # )

    spark = get_spark_session()

    REAL_ALLOCATION_SCENARIOS_TO_GENERATE = {
        f"{acceptance_approach}_acc_{arpu_approach}_arpu_{arpu_days}d": {
            "partition_columns": ["nba_spine_primary_key"],
            "model_group_columns": [model_group_column],
            "acceptance_column_str": "prediction_acceptance"
            if acceptance_approach == "pred"
            else "target_response",
            "arpu_column_str": f"prediction_arpu_{arpu_days}d"
            if arpu_approach == "pred"
            else f"target_relative_arpu_increase_{arpu_days}d",
        }
        for acceptance_approach, arpu_approach, in [
            ("real", "real"),
            ("real", "pred"),
            ("pred", "pred"),
        ]
        for arpu_days in ["7", "30"]
    }

    OPTIMAL_ALLOCATION_SCENARIOS_TO_GENERATE = {
        f"optimal_allocation_no_contact_restrictions_{arpu_days}d_arpu": {
            "partition_columns": ["nba_spine_primary_key"],  ## TODO be careful
            "model_group_columns": [model_group_column],
            "acceptance_column_str": "prediction_acceptance",
            "arpu_column_str": f"prediction_arpu_{arpu_days}d",
        }
        for arpu_days in ["7", "30"]
    }

    pdf_all_scenarios = pd.DataFrame()

    for scenario_name, scenario_params in REAL_ALLOCATION_SCENARIOS_TO_GENERATE.items():
        pdf_scenario = calculate_impact_scenario(
            df=df_master_with_scores,
            **scenario_params,
            calculate_optimal_allocation=False,
        )
        pdf_scenario.insert(0, "scenario", scenario_name)
        pdf_all_scenarios = pd.concat([pdf_all_scenarios, pdf_scenario])

    for (
        scenario_name,
        scenario_params,
    ) in OPTIMAL_ALLOCATION_SCENARIOS_TO_GENERATE.items():
        pdf_scenario = calculate_impact_scenario(
            df=df_master_expanded_with_scores,
            **scenario_params,
            calculate_optimal_allocation=True,
        )
        pdf_scenario.insert(0, "scenario", scenario_name)
        pdf_all_scenarios = pd.concat([pdf_all_scenarios, pdf_scenario])

    pdf_all_scenarios["n_arpu_days"] = pdf_all_scenarios["scenario"].str.extract(
        r"(\d+)"
    )
    pdf_all_scenarios = pdf_all_scenarios.sort_values(["model_group", "scenario"])
    df_all_scenarios = spark.createDataFrame(pdf_all_scenarios)

    return df_all_scenarios


def l5_nba_backtesting_pcm_eligible_spine(
    pcm_candidate: DataFrame,
    l5_nba_campaign_master: DataFrame,
    model_groups_to_score: List[str],
    prioritized_campaign_child_codes: List[str],
    nba_model_group_column_prioritized: str,
    nba_model_group_column_non_prioritized: str,
    min_feature_days_lag: int,
):

    # ##TODO remove
    # pcm_candidate = pcm_candidate.filter(F.col("candidate_date").isin(DATES_LIST))

    # Increase number of partitions when creating master table to avoid huge joins
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2000)

    # Remove duplicates and rename columns
    pcm_candidate = pcm_candidate.distinct().withColumnRenamed(
        "child_code", "campaign_child_code",
    )

    common_columns = list(
        set.intersection(
            set(l5_nba_campaign_master.columns), set(pcm_candidate.columns),
        )
    )
    if common_columns:
        logging.warning(
            f"There are common columns in pcm_contacts "
            f"and campaign_history_master_active: {', '.join(common_columns)}"
        )
        for common_column in common_columns:
            pcm_candidate = pcm_candidate.withColumnRenamed(
                common_column, common_column + "_from_pcm_contacts"
            )

    df_spine = pcm_candidate.join(
        F.broadcast(
            l5_nba_campaign_master.withColumnRenamed(
                "child_code", "campaign_child_code",
            )
        ),
        on="campaign_child_code",
        how="left",
    )

    df_spine = add_c360_dates_columns(
        df_spine,
        date_column="candidate_date",
        min_feature_days_lag=min_feature_days_lag,
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

    # Create a primary key for the master table spine
    df_spine = df_spine.withColumn(
        "nba_spine_primary_key",
        F.concat(
            F.col("subscription_identifier"),
            F.lit("_"),
            F.col("candidate_date"),
            F.lit("_"),
            F.col("campaign_child_code"),
        ),
    )

    df_spine = df_spine.filter(F.col("model_group").isin(model_groups_to_score))

    return df_spine
