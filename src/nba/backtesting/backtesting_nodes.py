from typing import Dict, List

import pai
import pandas as pd
import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

from customer360.utilities.spark_util import get_spark_session


def score_nba_models(
    df_master: pyspark.sql.DataFrame,
    primary_key_columns: List[str],
    model_group_column: str,
    models_to_score: Dict[str, str],
    scoring_chunk_size: int,
    pai_runs_uri: str,
    pai_artifacts_uri: str,
    explanatory_features: List[str] = None,
) -> pyspark.sql.DataFrame:
    # Define schema for the udf.
    schema = df_master.select(
        *(
            primary_key_columns
            + [
                F.lit(999.99).cast(DoubleType()).alias(prediction_colname)
                for prediction_colname in models_to_score.values()
            ]
        )
    ).schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def predict_pandas_udf(pdf):
        pai.set_config(
            storage_runs=pai_runs_uri, storage_artifacts=pai_artifacts_uri,
        )

        current_model_group = pdf[model_group_column].iloc[0]
        pd_results = pd.DataFrame()

        for current_tag, prediction_colname in models_to_score.items():
            current_run = pai.load_runs(
                experiment=current_model_group, tags=[current_tag]
            )
            assert (
                len(current_run) > 0
            ), f"There are no runs for expetiment {current_model_group} and tag {current_tag}"

            assert (
                len(current_run) < 2
            ), f"There are more than 1 runs for expetiment {current_model_group} and tag {current_tag}"

            current_run_id = current_run["run_id"].iloc[0]
            current_model = pai.load_model(run_id=current_run_id)
            df_current_model_features = pai.load_features(run_id=current_run_id)
            current_model_features = df_current_model_features["feature_list"].iloc[0]
            X = pdf[current_model_features]
            if "binary" in current_run["tags"].iloc[0]:
                pd_results[prediction_colname] = current_model.predict_proba(
                    X, num_threads=1, n_jobs=1
                )[:, 1]
            elif "regression" in current_run["tags"].iloc[0]:
                pd_results[prediction_colname] = current_model.predict(
                    X, num_threads=1, n_jobs=1
                )
            else:
                raise ValueError(
                    "Unrecognized model type while predicting, model has"
                    "neither 'binary' or 'regression' tags"
                )
            for pk_col in primary_key_columns:
                pd_results.loc[:, pk_col] = pdf.loc[:, pk_col]

        return pd_results

    df_master = df_master.withColumn(
        "partition",
        F.floor(
            F.count(F.lit(1)).over(Window.partitionBy(model_group_column))
            / scoring_chunk_size
            * F.rand()
        ),
    )
    if not explanatory_features:
        # Keep only necessary columns to make the pandas transformation more lightweight
        pai.set_config(
            storage_runs=pai_runs_uri, storage_artifacts=pai_artifacts_uri,
        )
        explanatory_features = set()
        for current_tag in models_to_score.keys():

            df_features = pai.load_features(tags=current_tag)
            current_model_features = set(
                [
                    f.replace("importance_", "")
                    for f in df_features.columns
                    if f.startswith("importance_")
                ]
            )
            explanatory_features = explanatory_features.union(current_model_features)
        explanatory_features = list(explanatory_features)

    df_master_necessary_columns = df_master.select(
        model_group_column,
        "partition",
        *(  # Don't add model group column twice in case it's a PK column
            list(set(primary_key_columns) - set([model_group_column]))
            + explanatory_features
        ),
    )

    df_scored = df_master_necessary_columns.groupby(
        model_group_column, "partition"
    ).apply(predict_pandas_udf)
    df_master_scored = df_master.join(df_scored, on=primary_key_columns, how="left")

    return df_master_scored


def calculate_impact_scenario(
    df: pyspark.sql.DataFrame,
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
                Window.partitionBy("nba_spine_primary_key").orderBy(
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
    df_master = df_master.sample(0.01)

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


def backtest_campaign_contacts(
    df_master_with_scores: pyspark.sql.DataFrame,
    df_master_expanded_with_scores: pyspark.sql.DataFrame,
    model_group_column: str,
) -> pyspark.sql.DataFrame:

    spark = get_spark_session()

    REAL_ALLOCATION_SCENARIOS_TO_GENERATE = {
        f"{acceptance_approach}_acc_{arpu_approach}_arpu_{arpu_days}d": {
            "model_group_columns": [model_group_column, "child_name"],
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
            "model_group_columns": [model_group_column, "child_name"],
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
