from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, get_spark_session

from pyspark.sql import DataFrame

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

    postpaid_revenue_df = data_non_availability_and_missing_check(df=prepaid_revenue_df, grouping="monthly",
                                                                  par_col="start_of_month",
                                                                  target_table_name="l4_revenue_ltv_to_date")

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
