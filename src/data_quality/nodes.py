from functools import partial, update_wrapper
from typing import *

from src.customer360.utilities.re_usable_functions import get_spark_session, get_spark_empty_df
from src.data_quality.dq_util import get_config_parameters, \
    get_dq_context, \
    get_partition_col, \
    get_dq_incremental_records, \
    get_dq_sampled_records, \
    melt_qa_result, \
    break_percentile_columns, \
    get_outlier_column, \
    add_most_frequent_value, \
    merge_all

from kedro.pipeline.node import Node
from kedro.io.core import DataSetError
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def sample_subscription_identifier(
        cust_profile_df: DataFrame,
        sample_size: int
) -> DataFrame:
    """
    Sample subscription_identifier from a dataframe
    Args:
        cust_profile_df: Dataframe with subscription_identifier from which we have to take sample
        sample_size: Number of samples we want to take

    Returns:
        Dataframe with unique subscription_identifier
    """
    spark = get_spark_session()

    cust_profile_df.createOrReplaceTempView("cust_profile_df")
    distinct_sub_id_df = spark.sql("""
        select 
            distinct(subscription_identifier) as subscription_identifier
        from cust_profile_df
        where subscription_identifier is not null
          and lower(subscription_identifier) != 'na'
    """)
    distinct_sub_id_count = distinct_sub_id_df.count()

    sample_fraction = min(sample_size / distinct_sub_id_count, 1.0)
    sampled_sub_id_df = distinct_sub_id_df.sample(withReplacement=False, fraction=sample_fraction)
    sampled_sub_id_df = sampled_sub_id_df.withColumn("created_date", F.current_date())

    return sampled_sub_id_df


def check_catalog_and_feature_exist(
        dq_config: dict
):
    ctx = get_dq_context()
    missing_files = []
    for dataset_name in dq_config.keys():
        try:
            df = ctx.catalog.load(dataset_name)

            for col in dq_config[dataset_name]:
                if col["feature"] not in df.columns:
                    missing_files.append("{}.{}".format(dataset_name, col))
        except:
            missing_files.append(dataset_name)

    if missing_files:
        err_msg = "These datasets/columns do not exist: {}".format(missing_files)
        raise FileNotFoundError(err_msg)


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


def run_accuracy_logic(
    input_df: DataFrame,
    sampled_sub_id_df: DataFrame,
    dq_config: dict,
    percentiles: Dict,
    dataset_name: str
) -> DataFrame:
    features_list = dq_config[dataset_name]

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

    partition_col = get_partition_col(input_df, dataset_name)

    ctx = get_dq_context()
    try:
        dq_accuracy_df = ctx.catalog.load("dq_accuracy")

        filtered_input_df = get_dq_incremental_records(
            input_df=input_df,
            dq_accuracy_df=dq_accuracy_df,
            dataset_name=dataset_name,
            partition_col=partition_col
        )
    except DataSetError:
        # no dq_accuracy table means the pipeline is never executed
        filtered_input_df = input_df

    if filtered_input_df.head() is None:
        return get_spark_empty_df(schema=dq_accuracy_df.schema)

    sampled_df = get_dq_sampled_records(filtered_input_df, sampled_sub_id_df)
    if sampled_df.head() is None:
        return get_spark_empty_df(schema=dq_accuracy_df.schema)

    sampled_df.createOrReplaceTempView("sampled_df")

    agg_features = []
    for each_feature in features_list:
        col = each_feature["feature"]
        for each_agg in agg_functions:
            agg_features.append(each_agg.format(col=col))

        if "outlier_formula" in each_feature:
            agg_features.append(get_outlier_column(each_feature))

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

    result_df = add_most_frequent_value(
        melted_result_df=result_df,
        features_list=features_list,
        partition_col=partition_col
    )

    if "percentiles" in result_df.columns:
        result_df = break_percentile_columns(result_df, percentiles["percentile_list"])

    result_df = (result_df
                 .withColumn("most_frequent_value_percentage", (F.col("most_freq_value_count")/F.col("count"))*100)
                 .withColumn("run_date", F.current_timestamp())
                 .withColumn("dataset_name", F.lit(dataset_name)))

    return result_df

