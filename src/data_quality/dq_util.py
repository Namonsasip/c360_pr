from kedro.pipeline.node import Node
from kedro.io.core import DataSetError
from pathlib import Path
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

import os
from typing import *
from functools import reduce, partial, update_wrapper

from src.customer360.utilities.re_usable_functions import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def get_dq_context():
    from src.customer360.run import DataQualityProjectContext
    return DataQualityProjectContext(Path.cwd(), env=conf)


def get_config_parameters(config_path="**/parameters.yml"):
    config = get_dq_context()._get_config_loader().get(config_path)
    return config


def generate_dq_nodes():
    nodes = []
    selected_dataset = get_config_parameters()['features_for_dq']
    for dataset_name, feature_list in selected_dataset.items():

        # Accuracy Test
        node = Node(
            func=update_wrapper(
                wrapper=partial(run_accuracy_logic, dataset_name=dataset_name),
                wrapped=run_accuracy_logic
            ),
            inputs=[dataset_name,
                    "dq_sampled_subscription_identifier",
                    "params:features_for_dq",
                    "params:percentiles"],
            outputs="dq_accuracy"
        )
        nodes.append(node)

    return nodes


def get_column_name_of_type(df, required_types=None):
    """
    Get all the column names of specific type
    """
    schema = [(x.name, str(x.dataType).split('(')[0]) for x in df.schema.fields]

    data_type = {
        'numeric': ["DecimalType", "DoubleType", "FloatType", "IntegerType", "LongType", "ShortType"],
        'string': ["StringType"],
        'boolean': ['BooleanType'],
        'date': ['DateType', 'TimestampType'],
        'array': ["ArrayType"]
    }

    if (required_types is None):
        return df.columns

    required_type_pyspark = []

    for required_type in required_types:
        required_type_pyspark = required_type_pyspark + data_type[required_type]

    return [column_name for column_name, data_type in schema if data_type in required_type_pyspark]


def run_accuracy_logic(
    input_df: DataFrame,
    sampled_sub_id_df: DataFrame,
    dq_config: dict,
    percentiles: Dict,
    dataset_name: str
) -> DataFrame:
    features_list = dq_config[dataset_name]

    possible_partition_date = [
        "event_partition_date",
        "start_of_week",
        "start_of_month"
    ]

    agg_functions = [
        "count({col}) as {col}__count",
        "avg({col}) as {col}__avg",
        "min({col}) as {col}__min",
        "max({col}) as {col}__max",
        "(sum(case when {col} is null then 1 else 0 end)/count(*))*100 as {col}__null_percentage",

        f"percentile_approx({{col}}, "
        f"array({','.join(map(str, percentiles['percentile_list']))}), {percentiles['accuracy']}) "
        f"as {{col}}__percentiles"
    ]

    partition_col = None
    for each_col in possible_partition_date:
        if each_col in input_df.columns:
            partition_col = each_col
            break

    ctx = get_dq_context()
    try:
        dq_accuracy_df = ctx.catalog.load("dq_accuracy")
        last_processed_date = (dq_accuracy_df
                               .filter(f.col("dataset_name") == dataset_name)
                               .select(f.max(f.col("corresponding_date")))
                               .head())

        if last_processed_date is not None:
            input_df = input_df.filter(f.col(partition_col) > last_processed_date[0])
            if input_df.head() is None:
                return get_spark_empty_df(schema=dq_accuracy_df.schema)
    except DataSetError:
        # no dq_accuracy table means the pipeline is never executed
        pass

    if partition_col is None:
        raise AttributeError("""No partition column is detected in dataset. 
        Available columns: {}""".format(input_df.columns))

    agg_features = []
    for each_feature in features_list:
        for each_agg in agg_functions:
            agg_features.append(each_agg.format(col=each_feature))

    # get only the latest sampled one
    max_sampled_date = sampled_sub_id_df.select(f.max(f.col("created_date"))).collect()[0][0]

    sampled_df = input_df.join(
        f.broadcast(sampled_sub_id_df.filter(f.col("created_date") == max_sampled_date)),
        on=["subscription_identifier"],
        how="inner"
    )
    sampled_df.createOrReplaceTempView("sampled_df")

    spark = get_spark_session()
    sql_stmt = """
        select {partition_col},
                {metrics}
        from sampled_df
        group by {partition_col}
    """.format(metrics=','.join(agg_features),
               partition_col=partition_col)

    result_df = spark.sql(sql_stmt)
    result_df = melt_qa_result(result_df, partition_col)

    # break percentiles into columns
    if "percentiles" in result_df.columns:
        for idx, percentile in enumerate(percentiles["percentile_list"]):
            result_df = result_df.withColumn(
                f"percentile_{percentile}",
                f.expr(f"case when percentiles is not null then percentiles[{idx}] else null end"))
        result_df = result_df.drop("percentiles")

    result_df = result_df.withColumn("run_date", f.current_timestamp())
    result_df = result_df.withColumn("dataset_name", f.lit(dataset_name))

    return result_df


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
