from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from kedro.context.context import load_context
from pathlib import Path
import os, logging
from pyspark.sql import DataFrame, functions as f

conf = os.getenv("CONF", None)


def union_weekly_cust_profile(
        cust_prof_daily_df: DataFrame,
        exception_partition=None

):
    ################################# Start Implementing Data availability checks #############################
    # if check_empty_dfs([cust_prof_daily_df]):
    #     return get_spark_empty_df()
    #
    # cust_prof_daily_df = data_non_availability_and_missing_check(df=cust_prof_daily_df, grouping="weekly",
    #                                                              par_col="event_partition_date",
    #                                                              missing_data_check_flg='Y',
    #                                                              target_table_name="l2_customer_profile_union_weekly_feature",
    #                                                              exception_partitions=exception_partition)
    #
    # if check_empty_dfs([cust_prof_daily_df]):
    #     return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    cust_prof_daily_df = cust_prof_daily_df.drop("start_of_month")

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

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    dates_list = cust_prof_daily_df.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    spark = get_spark_session()
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = cust_prof_daily_df.filter(f.col("start_of_week").isin(*[curr_item]))
        small_df.createOrReplaceTempView("cust_prof_daily_df")
        small_df = spark.sql(sql_stmt).drop("_rnk", "event_partition_date")
        CNTX.catalog.save("l2_customer_profile_union_weekly_feature", small_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = cust_prof_daily_df.filter(f.col("start_of_week").isin(*[first_item]))
    return_df.createOrReplaceTempView("cust_prof_daily_df")
    return_df = spark.sql(sql_stmt).drop("_rnk", "event_partition_date")

    return return_df

