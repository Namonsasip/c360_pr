from pyspark.sql import functions as f, DataFrame, Window
import pyspark
from src.data_quality.dq_util import melt, extract_domain
from typing import List


_in_thresh_expr = lambda x_col: f.when(x_col.isNotNull(), x_col).otherwise(f.lit(None))


def get_rolling_window(num_days):
    """
    Rolling window to look back num_days
    :param num_days: # of days
    :return:
    # """

    days = lambda i: (i - 1) * 86400
    w = (Window()
         .partitionBy("feature_column_name", "metric", "dataset_name")
         .orderBy(f.col("corresponding_date").cast("timestamp").cast("long"))
         .rangeBetween(-days(num_days), -1)
         )
    return w


def compute_iqr_thresholds(df: pyspark.sql.DataFrame, threshold_lookback_corresponding_dates: int) -> pyspark.sql.DataFrame:
    """
    Compute thresholds for accuracy, completeness and consistency dimension using IQR method.
    :param df: Dataframe with accuracy, completeness and consistency metrics.
    :param threshold_lookback_corresponding_dates: Number of corresponding dates to compare with previous correspond_dates.
    :return: Dataframe.
    """

    return (df
            .withColumn("quartile_1", f.expr("percentile_approx(value, 0.25)").over(get_rolling_window(threshold_lookback_corresponding_dates)))
            .withColumn("quartile_3", f.expr("percentile_approx(value, 0.75)").over(get_rolling_window(threshold_lookback_corresponding_dates)))
            .withColumn("iqr", f.col("quartile_3") - f.col("quartile_1"))
            .withColumn("lower_threshold", f.col("quartile_1") - (1.5 * f.col("iqr")))
            .withColumn("higher_threshold", f.col("quartile_3") + (1.5 * f.col("iqr")))
            .withColumn("extreme_lower_threshold", f.col("quartile_1") - (3 * f.col("iqr")))
            .withColumn("extreme_higher_threshold", f.col("quartile_3") + (3 * f.col("iqr")))
            .withColumnRenamed("value", "current_value")
            .withColumn("current_corresponding_date", f.to_timestamp(f.col("corresponding_date")))
            )


def apply_iqr_thresholds(df: pyspark.sql.DataFrame):

    # Define expressions

    expr_above_lower_thresh = f.col("current_value") > f.col("lower_threshold")
    expr_above_lower_extreme_threshold = f.col("current_value") > f.col("extreme_lower_threshold")

    expr_below_higher_threshold = f.col("current_value") < f.col("higher_threshold")
    expr_below_higher_extreme_threshold = f.col("current_value") < f.col("extreme_higher_threshold")

    expr_in_threshold = f.col("current_value").between(f.col("lower_threshold"), f.col("higher_threshold"))
    expr_outlier_type = f.when(~f.col("above_lower_extreme_threshold"), f.lit("lower_extreme_outlier")).otherwise(
        f.when(~f.col("below_higher_extreme_threshold"), f.lit("higher_extreme_outlier")).otherwise(
            f.when(~f.col("above_lower_threshold"), f.lit("lower_standard_outlier")).otherwise(
                f.when(~f.col("below_higher_threshold"), f.lit("higher_standard_outlier")).otherwise(
                    f.lit(None)
                )
            )
        )
    )

    # Apply expressions
    return (df
        .withColumn("above_lower_threshold", expr_above_lower_thresh)
        .withColumn("above_lower_extreme_threshold", expr_above_lower_extreme_threshold)
        .withColumn("below_higher_threshold", expr_below_higher_threshold)
        .withColumn("below_higher_extreme_threshold", expr_below_higher_extreme_threshold)
        .withColumn("in_threshold", expr_in_threshold)
        .withColumn("outlier_type", expr_outlier_type)
        .select(
            "corresponding_date",
            "feature_column_name",
            "metric",
            "current_value",
            "quartile_1",
            "quartile_3",
            "iqr",
            "higher_threshold",
            "lower_threshold",
            "extreme_higher_threshold",
            "extreme_lower_threshold",
            "in_threshold",
            "outlier_type",
            "dataset_name",
            "granularity",
            "run_date",
            "sub_id_sample_creation_date"
        )
    )


def compute_acc_com_dimensions(df: pyspark.sql.DataFrame, threshold_lookback_corresponding_dates: int) -> pyspark.sql.DataFrame:

    metrics = [
        'approx_count_distinct',
        'null_percentage',
        'min',
        'avg',
        'count',
        'max',
        '`percentile_0.1`',
        '`percentile_0.25`',
        '`percentile_0.5`',
        '`percentile_0.75`',
        '`percentile_0.9`',
        'count_higher_outlier',
        'q1',
        'iqr',
        'q3',
        'count_lower_outlier'
    ]

    melt_df = melt(
        df=df,
        id_vars=["corresponding_date", "feature_column_name", "dataset_name", "granularity", "run_date",
                 "sub_id_sample_creation_date"],
        value_vars=metrics,
        var_name="metric",
        value_name="value"
    )

    df_with_iqr = compute_iqr_thresholds(melt_df, threshold_lookback_corresponding_dates)
    df_with_thresh = apply_iqr_thresholds(df_with_iqr)

    return df_with_thresh


def pivot_threshold_output(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    grouping_cols = [
        "corresponding_date",
        "feature_column_name",
        "granularity",
        "dataset_name",
        "run_date",
        "domain",
        "sub_id_sample_creation_date"
    ]

    return df.groupBy(grouping_cols).pivot("metric").agg(
        _in_thresh_expr(f.first("in_threshold"))
    )


def group_threshold_output(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return (df
            .withColumn("in_auto_manual_threshold", _in_thresh_expr(f.col("in_threshold")))
            .groupBy("domain", "corresponding_date", "granularity")
            .agg(
                f.countDistinct("feature_column_name").alias("no_of_columns"),
                f.countDistinct("metric").alias("no_of_metrics"),
                f.count("in_auto_manual_threshold").alias("total_count"),
                f.count(f.when(f.col("in_auto_manual_threshold"), True)).alias("green_count"),
                f.count(f.when(~f.col("in_auto_manual_threshold"), True)).alias("red_count"),
            )
            .withColumn("percent_green", f.round(f.col("green_count") / (f.col("green_count") + f.col("red_count")), 4))
            .withColumn("percent_red", f.round(f.col("red_count") / (f.col("green_count") + f.col("red_count")), 4))
            .withColumn("run_date", f.current_date())
            )


def generate_dq_threshold_analysis(df: DataFrame, threshold_lookback_corresponding_dates: int) -> List[DataFrame]:

    acc_com_threshold = compute_acc_com_dimensions(df, threshold_lookback_corresponding_dates)
    acc_com_threshold_with_domain = acc_com_threshold.withColumn("domain", extract_domain(f.col("feature_column_name")))

    return [
        acc_com_threshold_with_domain,
        pivot_threshold_output(acc_com_threshold_with_domain),
        group_threshold_output(acc_com_threshold_with_domain)
    ]
