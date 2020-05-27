import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , union_dataframes_with_missing_cols, add_start_of_week_and_month
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def loyalty_number_of_points_balance(customer_prof: DataFrame
                                     , input_df: DataFrame
                                     , l3_loyalty_point_balance_statuses_monthly: dict) -> DataFrame:
    """
    :param customer_prof:
    :param input_df:
    :param l3_loyalty_point_balance_statuses_monthly:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="monthly", par_col="partition_month",
        target_table_name="l3_loyalty_point_balance_statuses_monthly")

    customer_prof = data_non_availability_and_missing_check(
        df=customer_prof, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_loyalty_point_balance_statuses_monthly")

    input_df = add_start_of_week_and_month(input_df=input_df, date_column="month_id")\
               .drop(["start_of_week", "event_partition_date"])

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            customer_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("start_of_month") <= min_value)
    customer_prof = customer_prof.filter(f.col("start_of_month") <= min_value)

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    join_key = ["access_method_num", "start_of_month"]
    customer_cols = ["national_id_card", "access_method_num", "subscription_identifier", "start_of_month"]
    customer_prof = customer_prof.select(customer_cols)

    input_df_temp = input_df.filter((f.col("msg_event_id").isin(33, 34)) &
                                    (f.upper(f.col("aunjai_flag").like('REGISTER%'))), f.col("response_date"))\
                            .groupBy("mobile_no", "start_of_month")\
                            .agg(f.max("response_date").alias("loyalty_register_program_points_date"))

    win = Window.partitionBy("mobile_no", "start_of_month").orderBy(f.col("response_date").desc())

    input_df = input_df.withColumn("rnk", f.row_number().over(win))\
                       .where("rnk = 1")
    merged_df = input_df.join(input_df_temp, ["start_of_month", "mobile_no"], how="left")

    merged_df = merged_df.select(f.col("mobile_no").alias("access_method_num")
                                            # "mobile_status_date"
                                            , "mobile_segment"
                                            , "points_balance_per_sub"
                                            , "point_expire_curr_year"
                                            , "point_expire_next_year"
                                            , "max_modified_date"
                                            , "max_expire_date"
                                            , "loyalty_register_program_points_date")

    merged_with_customer = merged_df.join(merged_df, join_key)
    return_df = node_from_config(merged_with_customer, l3_loyalty_point_balance_statuses_monthly)

    return return_df
