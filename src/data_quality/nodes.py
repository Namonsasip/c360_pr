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
    get_partition_count_formula, \
    melt, \
    add_suffix_to_df_columns, \
    add_outlier_percentage_based_on_iqr

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

    :param cust_profile_df: Dataframe with subscription_identifier from which we have to take sample
    :param sample_size: Number of samples we want to take
    :return: Dataframe with unique subscription_identifier
    """
    spark = get_spark_session()

    cust_profile_df.createOrReplaceTempView("cust_profile_df")
    distinct_sub_id_df = spark.sql("""
        select distinct(subscription_identifier) as subscription_identifier
        from cust_profile_df
    """)
    distinct_sub_id_count = distinct_sub_id_df.count()

    sample_fraction = min(sample_size / distinct_sub_id_count, 1.0)
    sampled_sub_id_df = distinct_sub_id_df.sample(withReplacement=False, fraction=sample_fraction)
    sampled_sub_id_df = sampled_sub_id_df.withColumn("created_date", F.current_date())

    return sampled_sub_id_df


def check_catalog_and_feature_exist(
        dq_config: dict
):
    """
    Returns a node which checks whether all catalogs and features defined in parameters exist in data source.

    :param dq_config:
    :return:
    """
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

    spark = get_spark_session()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", -1)
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")

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
    """
    Returns a list of node which includes: 1 node to compute accuracy and completeness dimension per dataset defined
    in parameters file and 1 node at the end to merge the outputs together in single dataframe.

    :param selected_dataset: Dictionary containing dataset_name and feature_list from parameters file.
    :return: List of nodes.
    """
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
                    "params:incremental_mode",
                    "all_catalog_and_feature_exist"
                    ],
            outputs=output_catalog,
            tags=["dq_accuracy_and_completeness",
                  "dq_accuracy",
                  "dq_completeness"]
        )

        nodes.append(node)
        accuracy_node_output_list.append(output_catalog)

    # Since node output must be unique, we create MemoryDataSet for each
    # node output above and then merge it with node below
    accuracy_merger_node = Node(
        func=dq_merger_nodes,
        inputs=accuracy_node_output_list,
        outputs="dq_accuracy_and_completeness",
        tags=["dq_accuracy_and_completeness",
              "dq_accuracy",
              "dq_completeness"]
    )
    nodes.append(accuracy_merger_node)
    return nodes


def _generate_availability_nodes(
        selected_dataset: Dict
) -> List[Node]:
    """
    Returns a list of node which includes: 1 node to compute availability dimension per dataset defined
    in parameters file and 1 node at the end to merge the outputs together in single dataframe.

    :param selected_dataset: Dictionary containing dataset_name and feature_list from parameters file.
    :return: List of nodes.
    """

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
            outputs=availability_output_catalog,
            tags=["dq_availability"]
        )

        nodes.append(availability_node)
        availability_node_output_list.append(availability_output_catalog)

    availability_merger_node = Node(
        func=dq_merger_nodes,
        inputs=availability_node_output_list,
        outputs="dq_availability",
        tags=["dq_availability"]
    )
    nodes.append(availability_merger_node)
    return nodes


def _generate_consistency_nodes(
        selected_dataset: Dict,
) -> List[Node]:
    """
    Returns a list of node which includes: 1 node to compute consistency dimension per dataset defined
    in parameters file and 1 node at the end to merge the outputs together in single dataframe.

    :param selected_dataset: Dictionary containing dataset_name and feature_list from parameters file.
    :return: List of nodes.
    """
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
            outputs=consistency_output_catalog,
            tags=["dq_consistency"]
        )
        nodes.append(consistency_node)
        consistency_node_output_list.append(consistency_output_catalog)

    consistency_merger_node = Node(
        func=dq_merger_nodes,
        inputs=consistency_node_output_list,
        outputs="dq_consistency",
        tags=["dq_consistency"]
    )
    nodes.append(consistency_merger_node)
    return nodes


def _generate_timeliness_nodes(
        selected_dataset: Dict,
) -> List[Node]:
    """
    Returns a list of node which includes: 1 node to compute timeliness dimension per dataset defined
    in parameters file and 1 node at the end to merge the outputs together in single dataframe.

    :param selected_dataset: Dictionary containing dataset_name and feature_list from parameters file.
    :return: List of nodes.
    """

    nodes = []
    timeliness_node_output_list = []
    for dataset_name, feature_list in selected_dataset.items():
        timeliness_output_catalog = "dq_timeliness_{}".format(dataset_name)
        timeliness_node = Node(
            func=update_wrapper(
                wrapper=partial(run_timeliness_logic, dataset_name=dataset_name),
                wrapped=run_timeliness_logic
            ),
            inputs=[dataset_name,
                    "all_catalog_and_feature_exist"],
            outputs=timeliness_output_catalog,
            tags=["dq_timeliness"]
        )
        nodes.append(timeliness_node)
        timeliness_node_output_list.append(timeliness_output_catalog)

    timeliness_merger_node = Node(
        func=dq_merger_nodes,
        inputs=timeliness_node_output_list,
        outputs="dq_timeliness",
        tags=["dq_timeliness"]
    )
    nodes.append(timeliness_merger_node)
    return nodes


def generate_dq_nodes():
    """
    Function to generate the data quality nodes for each dimension.

    :return: List containing all dimensions' nodes.
    """
    nodes = []
    selected_dataset = get_config_parameters()['features_for_dq']

    nodes.extend(_generate_accuracy_and_completeness_nodes(selected_dataset))
    nodes.extend(_generate_availability_nodes(selected_dataset))
    nodes.extend(_generate_consistency_nodes(selected_dataset))
    nodes.extend(_generate_timeliness_nodes(selected_dataset))

    return nodes


def run_accuracy_logic(
    input_df: DataFrame,
    sampled_sub_id_df: DataFrame,
    dq_config: dict,
    percentiles: Dict,
    incremental_mode: str,
    all_catalog_and_feature_exist: DataFrame,  # dependency to ensure this node runs after all checks are passed
    dataset_name: str
) -> DataFrame:
    """
    For a given dataset, this function computes the accuracy and completeness metrics. This function also takes in
    sampled_sub_id_df which is a sample of subscription_identifier we would like to run. This function also takes in
    an incremental_mode flag which either activates/deactivates incremental mode.

    :param input_df: Input dataframe.
    :param sampled_sub_id_df: Sample dataframe.
    :param dq_config: Features from input dataframe to create metrics upon.
    :param percentiles: Dictionary containing list of percentiles and an accuracy.
    :param incremental_mode: Incremental flag to activate/deactivate incremental mode.
    :param all_catalog_and_feature_exist: Unused, dependency to ensure this node runs after all checks are passed
    :param dataset_name: Dataset name.
    :return: Dataframe containing accuracy and completeness metrics.
    """
    dq_accuracy_df_schema = StructType([
        StructField("granularity", StringType()),
        StructField("feature_column_name", StringType()),
        StructField("approx_count_distinct", IntegerType()),
        StructField("null_percentage", DoubleType()),
        StructField("min", DoubleType()),
        StructField("avg", DoubleType()),
        StructField("count", IntegerType()),
        StructField("max", DoubleType()),
        StructField("percentile_0.1", DoubleType()),
        StructField("percentile_0.25", DoubleType()),
        StructField("percentile_0.5", DoubleType()),
        StructField("percentile_0.75", DoubleType()),
        StructField("percentile_0.9", DoubleType()),
        StructField("count_higher_outlier", DoubleType()),
        StructField("q1", DoubleType()),
        StructField("iqr", DoubleType()),
        StructField("q3", DoubleType()),
        StructField("count_lower_outlier", DoubleType()),
        StructField("run_date", TimestampType()),
        StructField("sub_id_sample_creation_date", DateType()),
        StructField("dataset_name", StringType()),
        StructField("corresponding_date", DateType())
    ])

    features_list = dq_config[dataset_name]
    features_list = replace_asterisk_feature(features_list, dataset_name, numeric_columns_only=True)

    agg_functions = [
        "cast(count({col}) as int) as {col}__count",
        "cast(avg({col}) as double) as {col}__avg",
        "cast(min({col}) as double) as {col}__min",
        "cast(max({col}) as double) as {col}__max",
        "cast((sum(case when {col} is null then 1 else 0 end)/count(*))*100 as double) as {col}__null_percentage",
        "cast(approx_count_distinct({col}) as int) as {col}__approx_count_distinct",
        "cast(count(*) as int) as {col}__count_all",

        f"percentile_approx({{col}}, "
        f"array({','.join(map(str, percentiles['percentile_list']))}), {percentiles['accuracy']}) "
        f"as {{col}}__percentiles"
    ]

    partition_col = get_partition_col(input_df, dataset_name)

    if incremental_mode.lower() == "on":

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
            return get_spark_empty_df(schema=dq_accuracy_df_schema)

    elif incremental_mode.lower() == "off":
        filtered_input_df = input_df

    else:
        raise Exception("Please specify 'on' or 'off' for 'incremental_mode' parameter.")

    sample_creation_date, sampled_df = get_dq_sampled_records(filtered_input_df, sampled_sub_id_df)
    if sampled_df.head() is None:
        return get_spark_empty_df(schema=dq_accuracy_df_schema)

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

    # this is to avoid running every process at the end which causes
    # long GC pauses before the spark job is even started
    # need to execute before so that percentiles don't move
    spark = get_spark_session()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", -1)
    result_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    result_df = add_outlier_percentage_based_on_iqr(
        raw_df=sampled_df,
        melted_df=result_df,
        partition_col=partition_col,
        features_list=features_list
    )

    result_with_outliers_df = (result_df
                               .withColumn("run_date", F.current_timestamp())
                               .withColumn("dataset_name", F.lit(dataset_name))
                               .withColumn("sub_id_sample_creation_date", F.lit(sample_creation_date))
                               .drop("count_all"))

    spark = get_spark_session()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", -1)
    result_with_outliers_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    return result_with_outliers_df.repartition(1)


def run_availability_logic(
        input_df: DataFrame,
        all_catalog_and_feature_exist: DataFrame,  # dependency to ensure this node runs after all checks are passed
        dataset_name: str
) -> DataFrame:
    """
    For a given dataset, this function computes the availability metrics.

    :param input_df: Input dataframe.
    :param all_catalog_and_feature_exist: Unused, dependency to ensure this node runs after all checks are passed
    :param dataset_name: Dataset name.
    :return: Dataframe containing availability metrics.
    """

    partition_col = get_partition_col(input_df, dataset_name)
    expected_partition_cnt_formula = get_partition_count_formula(partition_col=partition_col,
                                                                 date_end=f"max({partition_col})",
                                                                 date_start=f"min({partition_col})")

    input_df.createOrReplaceTempView("input_df")

    sql_stmt = """
        select
            cast('{partition_col}' as string) as granularity,
            cast(max({partition_col}) as string) as max_partition,
            cast(min({partition_col}) as string) as min_partition,
            count(distinct({partition_col})) as distinct_partition,
            cast(({expected_partition_cnt_formula}) - count(distinct({partition_col})) as integer) as missing_partition_count,
            cast('{dataset_name}' as string) as dataset_name,
            current_date() as run_date
        from input_df
    """.format(partition_col=partition_col,
               dataset_name=dataset_name,
               expected_partition_cnt_formula=expected_partition_cnt_formula.format(partition_col=partition_col))

    spark = get_spark_session()
    result_df = spark.sql(sql_stmt)

    return result_df.repartition(1)


def _prepare_dq_consistency_dataset(
        input_df: DataFrame,
        sampled_sub_id_df,
        dq_config: Dict,
        benchmark_start_date: str,
        benchmark_end_date: str,
        dataset_name: str
) -> DataFrame:
    """
    Function to prepare a dataset for consistency metrics. The preparation includes filtering the partitions to be
    in range of benchmark_start_date and benchmark_end_date. We also filter the subscription_identifier based on
    sample_sub_id_df.
    :param input_df: Input dataframe.
    :param sampled_sub_id_df: Sample dataframe.
    :param dq_config: Dictionary to get the features of a dataset.
    :param benchmark_start_date: Benchmark start date.
    :param benchmark_end_date: Benchmark end date.
    :param dataset_name: Dataset name.
    :return:
    """
    partition_col = get_partition_col(input_df, dataset_name)

    features_list = dq_config[dataset_name]
    features_list = replace_asterisk_feature(features_list, dataset_name)
    features_list = list(map(lambda x: x["feature"], features_list))

    filtered_new_df = (input_df
                       .filter((F.col(partition_col) >= F.to_date(F.lit(benchmark_start_date))) &
                               (F.col(partition_col) <= F.to_date(F.lit(benchmark_end_date))))
                       .select(*set(features_list + [partition_col, "subscription_identifier"])))

    sub_id_sampled_date, filtered_new_df = get_dq_sampled_records(filtered_new_df, sampled_sub_id_df)

    filtered_new_df = filtered_new_df.drop("created_date")

    return filtered_new_df.repartition(1)


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
    """
    Logic to compare current dataset to previously benchmarked dataset to calculate the same percentage.

    :param new_df: current data to compare with benchmark
    :param old_df: benchmarked data
    :param sampled_sub_id_df: dataframe containing list of sampled subscription_identifier
    :param dq_config: Configuration from parameters to get the features in a dataset.
    :param benchmark_start_date: Benchmark start date.
    :param benchmark_end_date: Benchmark end date.
    :param all_catalog_and_feature_exist: Unused, dependency to ensure this node runs after all checks are passed
    :param dataset_name: Dataset name.
    :return:
    """
    ctx = get_dq_context()
    partition_col = get_partition_col(new_df, dataset_name)
    granularity_cols = {partition_col, "subscription_identifier"}

    new_df = _prepare_dq_consistency_dataset(
        input_df=new_df,
        sampled_sub_id_df=sampled_sub_id_df,
        dq_config=dq_config,
        benchmark_start_date=benchmark_start_date,
        benchmark_end_date=benchmark_end_date,
        dataset_name=dataset_name
    )

    # if no benchmark is found, save current df as benchmark and save empty consistency result
    # because there's nothing to compare to
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

        # Check if old and new value are the same
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
                       .withColumn("same_percent", (F.col("same_percent")*100).cast(DoubleType()))
                       .withColumn("run_date", F.current_date())
                       .withColumn("dataset_name", F.lit(dataset_name)))

    # this is to avoid running every process at the end which causes
    # long GC pauses before the spark job is even started
    spark = get_spark_session()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.broadcastTimeout", -1)
    df_same_percent.persist(StorageLevel.MEMORY_AND_DISK).count()

    return df_same_percent.repartition(1)


def generate_latency_formula(
        partition_col: str
) -> str:
    """
    Helper function to return latency formula based on different partition columns.
    :param partition_col: Partition columns.
    :return: SQL string to execute latency formula.
    """
    if partition_col == 'event_partition_date' or partition_col == 'partition_date':
        return get_partition_count_formula(partition_col,
                                           date_end="current_date()",
                                           date_start=f"max({partition_col})")

    if partition_col == 'start_of_week':
        return get_partition_count_formula(partition_col,
                                           date_end="date_trunc('week', current_date())",
                                           date_start=f"max({partition_col})")

    return "{} - 1".format(get_partition_count_formula(partition_col,
                                                       date_end="date_trunc('month', current_date())",
                                                       date_start=f"max({partition_col})"))


def run_timeliness_logic(
        input_df: DataFrame,
        all_catalog_and_feature_exist: DataFrame,
        dataset_name: str
):
    """
    Function to run timeliness dimension.
    :param input_df: Input dataset.
    :param all_catalog_and_feature_exist: Unused, dependency to ensure this node runs after all checks are passed
    :param dataset_name: Dataset name.
    :return: Dataframe containing timeliness metrics.
    """
    input_df.createOrReplaceTempView("input_df")
    partition_col = get_partition_col(input_df, dataset_name)
    ctx = get_dq_context()
    spark = get_spark_session()

    initial_stats_for_input_df = """
        select
            cast('{dataset_name}' as string) as dataset_name, 
            cast('{partition_col}' as string) as granularity,
            cast(max({partition_col}) as string) as max_partition,
            cast({latency_formula} as string) as partition_latency,
            current_timestamp() as execution_ts,
            cast(0.0 as double) as latency_increase_from_last_run,
            current_date() as run_date
        from input_df
    """.format(latency_formula=generate_latency_formula(partition_col),
               dataset_name=dataset_name,
               partition_col=partition_col)

    try:
        dq_timeliness = ctx.catalog.load("dq_timeliness")
        dq_timeliness.createOrReplaceTempView("dq_timeliness")
    except:
        # no dq_timeliness yet, create initial stats
        result_df = spark.sql(initial_stats_for_input_df)
        return result_df

    result_df = spark.sql("""
        with unioned_df as (
            {initial_stats_for_input_df}
            union (
                select
                    cast(t1.dataset_name as string),
                    cast(t1.granularity as string),
                    cast(t1.max_partition as string),
                    cast(t1.partition_latency as string),
                    t1.execution_ts,
                    cast(t1.latency_increase_from_last_run as double),
                    t1.run_date
                from dq_timeliness t1
                where t1.dataset_name = '{dataset_name}'
                  and t1.run_date = (select max(run_date) from dq_timeliness t2)
            )
        )
        select
            unioned_df.dataset_name,
            unioned_df.granularity,
            unioned_df.max_partition,
            unioned_df.partition_latency,
            unioned_df.execution_ts,
            unioned_df.run_date,
            cast(
                coalesce(
                    partition_latency - lag(partition_latency, 1) over (partition by dataset_name order by execution_ts asc),
                    latency_increase_from_last_run
                ) as double
            ) as latency_increase_from_last_run
        from unioned_df
        
    """.format(initial_stats_for_input_df=initial_stats_for_input_df,
               latency_formula=generate_latency_formula(partition_col),
               dataset_name=dataset_name,
               partition_col=partition_col))

    return result_df
