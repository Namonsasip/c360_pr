from functools import partial, update_wrapper, reduce
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
    replace_asterisk_feature, \
    get_expected_partition_count_formula, \
    melt, \
    add_suffix_to_df_columns

from kedro.pipeline.node import Node
from kedro.io.core import DataSetError
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel


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
            distinct(crm_sub_id) as subscription_identifier
        from cust_profile_df
        where crm_sub_id is not null
          and lower(crm_sub_id) != 'na'
    """)
    distinct_sub_id_count = distinct_sub_id_df.count()

    sample_fraction = min(sample_size / distinct_sub_id_count, 1.0)
    sampled_sub_id_df = distinct_sub_id_df.sample(withReplacement=False, fraction=sample_fraction)
    sampled_sub_id_df = sampled_sub_id_df.withColumn("_sample_created_date", F.current_date())

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
                if col["feature"] != "*" and col["feature"] not in df.columns:
                    missing_files.append("{}.{}".format(dataset_name, col))
        except:
            missing_files.append(dataset_name)

    if missing_files:
        err_msg = "These datasets/columns do not exist: {}".format(missing_files)
        raise FileNotFoundError(err_msg)

    return get_spark_empty_df()


def dq_merger_nodes(
        *args: List[DataFrame]
) -> DataFrame:
    """
    Function to union all the output DataFrame.
    It will collect all columns and put default value as null
    if the DataFrame does not have that column

    :param args: List of DataFrame to be unioned
    :return: unioned DataFrame
    """

    all_cols = []
    for each_df in args:
        all_cols.extend(each_df.columns)
    all_cols = set(all_cols)  # remove duplicates

    df_list = list(args)

    for idx, each_df in enumerate(df_list):
        for each_col in all_cols:
            if each_col not in each_df.columns:
                df_list[idx] = df_list[idx].withColumn(each_col, F.lit(None))

    result_df = reduce(lambda x, y: x.unionByName(y), df_list)

    # to reduce number of files produced
    result_df = result_df.repartition(len(args), "dataset_name")

    return result_df


def _generate_accuracy_and_completeness_nodes(
        selected_dataset: Dict
) -> List[Node]:
    accuracy_node_output_list = []
    nodes = []

    for dataset_name, feature_list in selected_dataset.items():
        output_catalog = "dq_accuracy_{}".format(dataset_name)
        node = Node(
            func=update_wrapper(
                wrapper=partial(run_accuracy_logic, dataset_name=dataset_name),
                wrapped=run_accuracy_logic
            ),
            inputs=[dataset_name,
                    "dq_sampled_subscription_identifier",
                    "params:features_for_dq",
                    "params:percentiles",
                    "all_catalog_and_feature_exist"
                    ],
            outputs=output_catalog
        )

        nodes.append(node)
        accuracy_node_output_list.append(output_catalog)

    # Since node output must be unique, we create MemoryDataSet for each
    # node output above and then merge it with node below
    accuracy_merger_node = Node(
        func=dq_merger_nodes,
        inputs=accuracy_node_output_list,
        outputs="dq_accuracy_and_completeness"
    )
    nodes.append(accuracy_merger_node)
    return nodes


def _generate_availability_nodes(
        selected_dataset: Dict
) -> List[Node]:
    nodes = []
    availability_node_output_list = []

    for dataset_name, feature_list in selected_dataset.items():
        availability_output_catalog = "dq_availability_{}".format(dataset_name)
        availability_node = Node(
            func=update_wrapper(
                wrapper=partial(run_availability_logic, dataset_name=dataset_name),
                wrapped=run_availability_logic
            ),
            inputs=[dataset_name,
                    "all_catalog_and_feature_exist"],
            outputs=availability_output_catalog
        )

        nodes.append(availability_node)
        availability_node_output_list.append(availability_output_catalog)

    availability_merger_node = Node(
        func=dq_merger_nodes,
        inputs=availability_node_output_list,
        outputs="dq_availability"
    )
    nodes.append(availability_merger_node)
    return nodes


def _generate_consistency_nodes(
        selected_dataset: Dict,
) -> List[Node]:
    nodes = []
    consistency_node_output_list = []
    for dataset_name, feature_list in selected_dataset.items():
        consistency_output_catalog = "dq_consistency_{}".format(dataset_name)
        consistency_node = Node(
            func=update_wrapper(
                wrapper=partial(run_consistency_logic, dataset_name=dataset_name),
                wrapped=run_consistency_logic
            ),
            inputs=[dataset_name,
                    "dq_consistency_benchmark_{}".format(dataset_name),
                    "dq_sampled_subscription_identifier",
                    "params:features_for_dq",
                    "params:benchmark_start_date",
                    "params:benchmark_end_date",
                    "all_catalog_and_feature_exist"],
            outputs=consistency_output_catalog
        )
        nodes.append(consistency_node)
        consistency_node_output_list.append(consistency_output_catalog)

    consistency_merger_node = Node(
        func=dq_merger_nodes,
        inputs=consistency_node_output_list,
        outputs="dq_consistency"
    )
    nodes.append(consistency_merger_node)
    return nodes


def generate_dq_nodes():
    nodes = []
    selected_dataset = get_config_parameters()['features_for_dq']

    # nodes.extend(_generate_accuracy_and_completeness_nodes(selected_dataset))
    # nodes.extend(_generate_availability_nodes(selected_dataset))
    nodes.extend(_generate_consistency_nodes(selected_dataset))

    return nodes


def run_accuracy_logic(
    input_df: DataFrame,
    sampled_sub_id_df: DataFrame,
    dq_config: dict,
    percentiles: Dict,
    all_catalog_and_feature_exist: DataFrame,  # dependency to ensure this node runs after all checks are passed
    dataset_name: str
) -> DataFrame:
    features_list = dq_config[dataset_name]
    features_list = replace_asterisk_feature(features_list, dataset_name, numeric_columns_only=True)

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
        dq_accuracy_df = ctx.catalog.load("dq_accuracy_and_completeness")

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

    sample_creation_date, sampled_df = get_dq_sampled_records(filtered_input_df, sampled_sub_id_df)
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

    # result_df = add_most_frequent_value(
    #     melted_result_df=result_df,
    #     features_list=features_list,
    #     partition_col=partition_col
    # )

    if "percentiles" in result_df.columns:
        result_df = break_percentile_columns(result_df, percentiles["percentile_list"])

    result_df = (result_df
                 # .withColumn("most_frequent_value_percentage", (F.col("most_freq_value_count")/F.col("count"))*100)
                 .withColumn("run_date", F.current_timestamp())
                 .withColumn("dataset_name", F.lit(dataset_name))
                 .withColumn("sub_id_sample_creation_date", F.lit(sample_creation_date)))

    # this is to avoid running every process at the end which causes
    # long GC pauses before the spark job is even started
    result_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    return result_df


def run_availability_logic(
        input_df: DataFrame,
        all_catalog_and_feature_exist: DataFrame,  # dependency to ensure this node runs after all checks are passed
        dataset_name: str
) -> DataFrame:

    partition_col = get_partition_col(input_df, dataset_name)
    expected_partition_cnt_formula = get_expected_partition_count_formula(partition_col)

    input_df.createOrReplaceTempView("input_df")

    sql_stmt = """
        select
            '{partition_col}' as granularity,
            max({partition_col}) as max_partition,
            min({partition_col}) as min_partition,
            count(distinct({partition_col})) as distinct_partition,
            cast(({expected_partition_cnt_formula}) - count(distinct({partition_col})) as integer) as missing_partition_count,
            '{dataset_name}' as dataset_name,
            current_date() as run_date
        from input_df
    """.format(partition_col=partition_col,
               dataset_name=dataset_name,
               expected_partition_cnt_formula=expected_partition_cnt_formula.format(partition_col=partition_col))

    spark = get_spark_session()
    result_df = spark.sql(sql_stmt)

    return result_df


def run_consistency_logic(
        new_df: DataFrame,
        old_df: DataFrame,
        sampled_sub_id_df: DataFrame,
        dq_config: dict,
        benchmark_start_date: str,
        benchmark_end_date: str,
        all_catalog_and_feature_exist: DataFrame,  # dependency to ensure this node runs after all checks are passed
        dataset_name: str
):
    ctx = get_dq_context()
    partition_col = get_partition_col(new_df, dataset_name)
    granularity_cols = {partition_col, "subscription_identifier"}

    features_list = dq_config[dataset_name]
    features_list = replace_asterisk_feature(features_list, dataset_name)
    features_list = list(map(lambda x: x["feature"], features_list))

    new_df = (new_df
              .filter((F.col(partition_col) >= F.to_date(F.lit(benchmark_start_date))) &
                      (F.col(partition_col) <= F.to_date(F.lit(benchmark_end_date))))
              .select(*set(features_list + list(granularity_cols))))

    sub_id_sampled_date, new_df = get_dq_sampled_records(new_df, sampled_sub_id_df)

    if old_df.head() is None or len(old_df.head()) == 0:
        new_df = new_df.withColumnRenamed(partition_col, "corresponding_date")
        ctx.catalog.save(f"dq_consistency_benchmark_{dataset_name}", new_df)

        return get_spark_empty_df(schema=StructType([StructField("dataset_name", StringType(), True),
                                                     StructField("run_date", DateType(), True),
                                                     StructField("feature_column_name", StringType(), True)]))

    old_df = old_df.withColumnRenamed("corresponding_date", partition_col)

    new_df_cols = new_df.columns
    old_df_cols = old_df.columns
    columns_exist_in_both = set(new_df_cols).intersection(set(old_df_cols))
    columns_not_exist_in_both = set(new_df_cols).symmetric_difference(set(old_df_cols))

    # If subscription_identifier and partition_col is missing raise error
    if not all(var in list(columns_exist_in_both) for var in granularity_cols):
        raise ValueError(f"subscription_identifier or {partition_col} is missing for dataset: {dataset_name}!")

    df_old_new_merged = None

    # Select common columns
    new_df = new_df.select(*columns_exist_in_both)
    old_df = old_df.select(*columns_exist_in_both)

    common_columns_without_granularity = list(columns_exist_in_both - granularity_cols)
    
    if len(common_columns_without_granularity) != 0:

        # Add old and new suffix to columns and join for same msisdn and weekstart
        new_df = add_suffix_to_df_columns(new_df, "_new", columns=common_columns_without_granularity)
        old_df = add_suffix_to_df_columns(old_df, "_old", columns=common_columns_without_granularity)

        # use outer because we want to detect missing partition (or duplicated granularity) as well
        df_old_new_merged = new_df.join(old_df, on=list(granularity_cols), how="outer")

        def _is_same(col_name):
            return ((F.isnull(F.col(f"{col_name}_old")) & F.isnull(F.col(f"{col_name}_new"))) |
                    (F.col(f"{col_name}_old") == F.col(f"{col_name}_new"))).cast(IntegerType())

        # Check if old and new value are same or null
        for column in common_columns_without_granularity:
            df_old_new_merged = df_old_new_merged.withColumn(f"{column}_is_eq", _is_same(column))

        df_old_new_merged = (df_old_new_merged
                             .select(list(granularity_cols)
                                     + [F.col(col) for col in df_old_new_merged.columns if col.endswith("_is_eq")]))

        df_old_new_merged = df_old_new_merged.na.fill(0.0)

    # We checked this for added or removed columns
    if len(columns_not_exist_in_both) != 0:

        # this happened if the columns are completely changed
        # except for subscription_identifier and partition_col
        # (already selected above)
        if df_old_new_merged is None:
            df_old_new_merged = new_df
            
        for each_col in columns_not_exist_in_both:
            df_old_new_merged = df_old_new_merged.withColumn(f"{each_col}_is_eq", F.lit(0))

    # Count the number or same records for each column
    df_same_percent = (df_old_new_merged
                       .groupby(partition_col)
                       .agg(*[F.mean(col).alias(f"{col.replace('_is_eq', '')}__same_percent")
                              for col in df_old_new_merged.columns if col.endswith("_is_eq")]))

    df_same_percent = melt_qa_result(df_same_percent, partition_col)

    df_same_percent = (df_same_percent
                       .withColumn("run_date", F.current_date())
                       .withColumn("dataset_name", F.lit(dataset_name)))

    # this is to avoid running every process at the end which causes
    # long GC pauses before the spark job is even started
    df_same_percent.persist(StorageLevel.MEMORY_AND_DISK).count()

    return df_same_percent
