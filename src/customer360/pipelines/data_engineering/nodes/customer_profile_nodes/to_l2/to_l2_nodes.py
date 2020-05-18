from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check

from pyspark.sql import DataFrame, functions as f


def union_weekly_cust_profile(
        cust_prof_daily_df: DataFrame
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cust_prof_daily_df]):
        return get_spark_empty_df()

    cust_prof_daily_df = data_non_availability_and_missing_check(df=cust_prof_daily_df, grouping="weekly",
                                                                 par_col="event_partition_date",
                                                                 missing_data_check_flg='Y',
                                                                 target_table_name="l2_customer_profile_union_weekly_feature")

    if check_empty_dfs([cust_prof_daily_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    cust_prof_daily_df.createOrReplaceTempView("cust_prof_daily_df")

    sql_stmt = """
        with ranked_cust_profile as (
            select 
                *,
                row_number() over (partition by subscription_identifier, start_of_week
                                    order by event_partition_date desc) as _rnk
            from cust_prof_daily_df
        )
        select *
        from ranked_cust_profile
        where _rnk = 1
    """

    spark = get_spark_session()
    df = spark.sql(sql_stmt)
    df = df.drop("_rnk")

    return df

