from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    get_spark_session, union_dataframes_with_missing_cols
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.config_parser import node_from_config
from pyspark.sql import DataFrame, functions as f
from pyspark.storagelevel import StorageLevel


def df_copy_for_l4_customer_profile_ltv_to_date(input_df):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l4_customer_profile_ltv_to_date")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def calculate_ltv_to_date(
        prepaid_revenue_df: DataFrame,
        postpaid_revenue_df: DataFrame
) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([prepaid_revenue_df, postpaid_revenue_df]):
        return get_spark_empty_df()

    prepaid_revenue_df = data_non_availability_and_missing_check(df=prepaid_revenue_df, grouping="monthly",
                                                                 par_col="start_of_month",
                                                                 target_table_name="l4_revenue_ltv_to_date")

    postpaid_revenue_df = data_non_availability_and_missing_check(df=postpaid_revenue_df, grouping="monthly",
                                                                  par_col="start_of_month",
                                                                  target_table_name="l4_revenue_ltv_to_date")

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            prepaid_revenue_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            postpaid_revenue_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    prepaid_revenue_df = prepaid_revenue_df.filter(f.col("start_of_month") <= min_value)
    postpaid_revenue_df = postpaid_revenue_df.filter(f.col("start_of_month") <= min_value)

    if check_empty_dfs([prepaid_revenue_df, postpaid_revenue_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    identifier = ["subscription_identifier",
                  "access_method_num",
                  "national_id_card"]
    granularity_col = identifier + ["start_of_month"]

    prepaid_revenue_df = prepaid_revenue_df.select(granularity_col + ["rev_arpu_total_revenue"])
    postpaid_revenue_df = postpaid_revenue_df.select(granularity_col + ["rev_arpu_total_revenue"])

    combined_revenue_df = prepaid_revenue_df.unionByName(postpaid_revenue_df)

    combined_revenue_df.createOrReplaceTempView("combined_revenue_df")

    spark = get_spark_session()
    df = spark.sql("""
        with combined_rpu as (
            select
                {granularity_col},
                sum(rev_arpu_total_revenue) as rev_arpu_total_revenue
            from combined_revenue_df
            group by {granularity_col}
        ) 
        select 
            {granularity_col},
            sum(rev_arpu_total_revenue) over (partition by {identifier}
                                              order by start_of_month asc) as ltv_to_date
        from combined_rpu
    """.format(granularity_col=", ".join(granularity_col),
               identifier=", ".join(identifier)))

    return df


def revenue_l4_dataset_monthly_datasets(input_df: DataFrame,
                                        rolling_window_dict: dict,
                                        node_from_config_dict: dict
                                        ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict:
    :param node_from_config_dict:
    :return:
    """
    rolling_df = l4_rolling_window(input_df, rolling_window_dict)
    #node_df = node_from_config(rolling_df, node_from_config_dict)
    return rolling_df
