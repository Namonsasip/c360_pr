from pathlib import Path
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType, StringType

import os
from datetime import datetime
from typing import *
from functools import reduce
from itertools import chain

from src.customer360.utilities.re_usable_functions import get_spark_session

conf = os.getenv("CONF", None)


def get_dq_context():
    from src.customer360.run import DataQualityProjectContext
    return DataQualityProjectContext(Path.cwd(), env=conf)


def get_config_parameters(config_path="**/parameters.yml"):
    config = get_dq_context()._get_config_loader().get(config_path)
    return config


def get_partition_col(
    input_df: DataFrame,
    dataset_name: str
) -> str:
    possible_partition_date = [
        "event_partition_date",
        "start_of_week",
        "start_of_month",
        "partition_month",
        "partition_date",
    ]

    partition_col = None
    for each_col in possible_partition_date:
        if each_col in input_df.columns:
            partition_col = each_col
            break

    if partition_col is None:
        raise AttributeError("""No partition column is detected in dataset: {}. 
        Available columns: {}""".format(dataset_name, input_df.columns))

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

    if last_processed_date is not None and last_processed_date[0] is not None:
        input_df = input_df.filter(f.col(partition_col) > last_processed_date[0])

    return input_df


def get_dq_sampled_records(
        input_df: DataFrame,
        sampled_sub_id_df: DataFrame,
) -> Tuple[datetime, DataFrame]:
    # get only the latest sampled one
    max_sampled_date = sampled_sub_id_df.select(f.max(f.col("created_date"))).collect()[0][0]

    sampled_df = input_df.join(
        f.broadcast(sampled_sub_id_df.filter(f.col("created_date") == max_sampled_date)),
        on=["subscription_identifier"],
        how="inner"
    )

    return max_sampled_date, sampled_df


def break_percentile_columns(
        input_df: DataFrame,
        percentile_list: List[float]
) -> DataFrame:
    for idx, percentile in enumerate(percentile_list):
        input_df = input_df.withColumn(
            f"percentile_{percentile}",
            f.expr(f"case when percentiles is not null then cast(percentiles[{idx}] as double) else null end")
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


def get_outlier_column(
    feature_config: Dict
):
    col = feature_config["feature"]
    return f"({feature_config['outlier_formula'].format(col=col)}/count(*))*100 " \
           f"as {col}__outlier_percentage"


def add_most_frequent_value(
        melted_result_df: DataFrame,
        features_list: List[Dict],
        partition_col: str
) -> DataFrame:
    spark = get_spark_session()

    most_freq_df = None
    for each_feature in features_list:
        if not each_feature.get("calculate_most_freq_value"):
            continue

        most_freq_sql_stmt = """
            select {col} as {col}__most_freq_value, 
                   {partition_col} as {partition_col}, 
                   {col}__count as {col}__most_freq_value_count
            from (
                select {col}, 
                       {partition_col},  
                       count(*) as {col}__count,
                       row_number() over (partition by {partition_col} 
                                          order by count(*) desc) as rnk
                from sampled_df
                group by {col}, {partition_col}  
            ) t
            where rnk = 1

        """.format(col=each_feature["feature"],
                   partition_col=partition_col)
        df = spark.sql(most_freq_sql_stmt)
        df = melt_qa_result(df, partition_col)

        if most_freq_df is None:
            most_freq_df = df
        else:
            most_freq_df = most_freq_df.unionByName(df)

    if most_freq_df is not None:
        melted_result_df = merge_all(
            dfs=[melted_result_df, most_freq_df],
            how="outer",
            on=["corresponding_date", "granularity", "feature_column_name"]
        )
        melted_result_df = (melted_result_df
                            .withColumn("most_frequent_value_percentage",
                                        ((f.col("most_freq_value_count")/f.col("count_all"))*100).cast(DoubleType())))

    return melted_result_df


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


def replace_asterisk_feature(
        features_list: List[Dict],
        dataset_name: str,
        numeric_columns_only=False
) -> List[Dict]:
    """
    replace `feature: '*'` with all the columns in the dataset

    :param features_list: take from dq parameters.yml
    :param dataset_name: name of dataset to retrieve
    :param numeric_columns_only: filter out all non numeric columns

    :return: modified features_list if `feature: '*'` is found
    """

    feature_name_set = set(map(lambda x: x["feature"], features_list))

    if "*" not in feature_name_set:
        return features_list

    result_list = list(filter(lambda x: x["feature"] != "*", features_list))

    ctx = get_dq_context()
    df = ctx.catalog.load(dataset_name)

    column_list = get_column_name_of_type(df, ["numeric"] if numeric_columns_only else None)

    for each_col in column_list:
        if each_col not in feature_name_set:
            result_list.append({"feature": each_col})

    return result_list


def get_partition_count_formula(
        partition_col: str,
        date_end: str,
        date_start: str
) -> str:

    """
    given a partition column name, get the formula to get expected
    partition count between two values

    :param partition_col: partition column name (event_partition_date,
            start_of_week, start_of_month, or partition_month),
    :param date_end: end date/partition for the formula
    :param date_start: start date/partition for the formula
    """

    expected_partition_cnt_formula = None
    if partition_col == 'event_partition_date' or partition_col == 'partition_date':
        expected_partition_cnt_formula = f'datediff({date_end}, {date_start}) + 1'
    elif partition_col == 'start_of_week':
        expected_partition_cnt_formula = f'(datediff({date_end}, {date_start}) / 7) + 1'
    elif partition_col == 'start_of_month' or 'partition_month':
        expected_partition_cnt_formula = f'months_between({date_end}, {date_start}) + 1'

    return expected_partition_cnt_formula


def add_suffix_to_df_columns(
        df: DataFrame,
        suffix: str,
        columns: list = None
) -> DataFrame:
    """
    Add suffix to the column names of dataframe
    Args:
        df (pyspark.sql.DataFrame): Dataframe on which we add suffix on columns
        suffix (str): Suffix
        columns (list): Columns on which we want to add suffix. Default: None

    Returns:
        Dataframe with columns renamed with suffix added
    """
    # If columns list are not provided all suffix on all the columns of dataframe
    if not columns:
        columns = df.columns

    for column in columns:
        df = df.withColumnRenamed(column, column + suffix)

    return df


def add_outlier_percentage_based_on_iqr(
        raw_df: DataFrame,
        melted_df: DataFrame,
        partition_col: str,
        features_list: List[Dict]
):
    modified_melted_df = (melted_df
                          .select("corresponding_date", "feature_column_name", "`percentile_0.25`", "`percentile_0.75`")
                          .groupBy("corresponding_date")
                          .pivot("feature_column_name")
                          .agg(
                                f.first(f.col("`percentile_0.25`")).alias("_q1"),
                                f.first(f.col("`percentile_0.75`")).alias("_q3"),
                                (f.first(f.col("`percentile_0.75`")) - f.first(f.col("`percentile_0.25`"))).alias("_iqr"),
                          )
                          .withColumnRenamed("corresponding_date", partition_col))

    joined_df = raw_df.join(f.broadcast(modified_melted_df), on=partition_col, how="inner")
    feature_list = list(map(lambda x: x["feature"], features_list))

    outlier_results = joined_df.groupBy([partition_col]).agg(
        *[
            f.count(
                f.when(f.col(num_col) <
                       (f.col(f"{num_col}__q1") - (1.5 * (f.col(f"{num_col}__iqr")))), 1)
            ).cast(DoubleType()).alias(f"{num_col}__count_lower_outlier")
            for num_col in feature_list
        ],
        *[
            f.count(
                f.when(f.col(num_col) >
                       (f.col(f"{num_col}__q3") + (1.5 * (f.col(f"{num_col}__iqr")))), 1)
            ).cast(DoubleType()).alias(f"{num_col}__count_higher_outlier")
            for num_col in feature_list
        ],
        *list(chain.from_iterable([
            [f.first(f.col(f"{num_col}__q1")).cast(DoubleType()).alias(f"{num_col}__q1"),
             f.first(f.col(f"{num_col}__q3")).cast(DoubleType()).alias(f"{num_col}__q3"),
             f.first(f.col(f"{num_col}__iqr")).cast(DoubleType()).alias(f"{num_col}__iqr")]
            for num_col in feature_list
        ]))
    )

    outlier_results_melted = melt_qa_result(outlier_results, partition_col)

    result_df = merge_all(
        [melted_df, outlier_results_melted],
        how="outer",
        on=["corresponding_date", "granularity", "feature_column_name"]
    )

    # Because approx_percentile is an approximation function (with accuracy percentage) and due to Spark's lazy
    # evaluation, occasionally the percentiles get re-calculated on 'action' functions and generates different values.
    # This fix keeps q1/pecentile_0.25 consistent and q3/percentile_0.75 consistent

    result_df = (result_df
                 .withColumn("percentile_0.25", f.col("q1"))
                 .withColumn("percentile_0.75", f.col("q3"))
                 )

    return result_df


fea_to_domain_dict = {
    "payment": "billing",
    "campaign": "campaign",
    "complaint": "complaint",
    "device": "device",
    "digital": "digital",
    "loyalty": "loyalty",
    "network": "network",
    "product": "product",
    "customer": "customer profile",
    "arpu": "revenue",
    "transaction": "sales",
    "visit_count": "stream",
    "download_kb_traffic": "stream",
    "session_duration": "stream",
    "touchpoints": "touchpoints",
    "usg": "usage"
}


@udf(returnType=StringType())
def extract_domain(col_name):
    for fea, domain in fea_to_domain_dict.items():
        if fea in col_name:
            return domain
    return "uncategorized"
