from pathlib import Path
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

import os
from typing import *
from functools import reduce


conf = os.getenv("CONF", None)


def get_dq_context():
    from src.customer360.run import DataQualityProjectContext
    return DataQualityProjectContext(Path.cwd(), env=conf)


def get_config_parameters(config_path="**/parameters.yml"):
    config = get_dq_context()._get_config_loader().get(config_path)
    return config


def get_partition_col(
    input_df: DataFrame
) -> str:
    possible_partition_date = [
        "event_partition_date",
        "start_of_week",
        "start_of_month"
    ]

    partition_col = None
    for each_col in possible_partition_date:
        if each_col in input_df.columns:
            partition_col = each_col
            break

    if partition_col is None:
        raise AttributeError("""No partition column is detected in dataset. 
        Available columns: {}""".format(input_df.columns))

    return partition_col


def get_dq_incremental_records(
    input_df: DataFrame,
    dq_accuracy_df: DataFrame,
    dataset_name: str,
    partition_col: str
) -> DataFrame:
    last_processed_date = (dq_accuracy_df
                           .filter(f.col("dataset_name") == dataset_name)
                           .select(f.max(f.col("corresponding_date")))
                           .head())

    if last_processed_date is not None:
        input_df = input_df.filter(f.col(partition_col) > last_processed_date[0])

    return input_df


def get_dq_sampled_records(
        input_df: DataFrame,
        sampled_sub_id_df: DataFrame
) -> DataFrame:
    # get only the latest sampled one
    max_sampled_date = sampled_sub_id_df.select(f.max(f.col("created_date"))).collect()[0][0]

    sampled_df = input_df.join(
        f.broadcast(sampled_sub_id_df.filter(f.col("created_date") == max_sampled_date)),
        on=["subscription_identifier"],
        how="inner"
    )

    return sampled_df


def break_percentile_columns(
        input_df: DataFrame,
        percentile_list: List[float]
) -> DataFrame:
    for idx, percentile in enumerate(percentile_list):
        input_df = input_df.withColumn(
            f"percentile_{percentile}",
            f.expr(f"case when percentiles is not null then percentiles[{idx}] else null end")
        )
    input_df = input_df.drop("percentiles")

    return input_df


def melt_qa_result(
        qa_result: DataFrame,
        partition_col: str
):
    """
    Convert QA result into multiple rows such that feature columns are rows and columns are metrics
    Args:
        qa_result:
        partition_col:
    Returns:
        Melted dataframe where columns are metrics used for QA Check
    """
    metrics = set([col.split("__")[-1] for col in qa_result.columns if "__" in col])

    qa_check_aggregates = []

    for metric in metrics:
        cols = [col for col in qa_result.columns if col.endswith("__{}".format(metric))]
        qa_result_aggregate = qa_result.select(
            [f.col(partition_col)]
            + [f.col(c).alias(c.replace(f"__{metric}", "")) for c in cols]
        )
        result_melted = melt(
            df=qa_result_aggregate,
            id_vars=[partition_col],
            value_vars=list(set(qa_result_aggregate.columns) - set([partition_col])),
            var_name="feature_column_name",
            value_name=metric,
        )
        result_melted = (result_melted
                         .withColumn("granularity", f.lit(partition_col))
                         .withColumnRenamed(partition_col, "corresponding_date"))

        qa_check_aggregates.append(result_melted)

    qa_result_melted = merge_all(
        qa_check_aggregates,
        how="outer",
        on=["corresponding_date", "granularity", "feature_column_name"]
    )

    return qa_result_melted


def melt(
        df: DataFrame,
        id_vars: Iterable[str],
        value_vars: Iterable[str],
        var_name: str = "variable",
        value_name: str = "value"
) -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = f.array(*(
        f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = id_vars + [f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def merge_all(dfs, on, how='inner'):
    """
    Merge all the dataframes
    """
    return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)
