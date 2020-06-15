from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, \
     union_dataframes_with_missing_cols, gen_max_sql, execute_sql
from customer360.utilities.config_parser import l4_rolling_window
from pyspark.sql import DataFrame, functions as f
import os

conf = os.getenv("CONF", None)


def sales_l4_rolling_window(input_df: DataFrame,
                            rolling_window_dict_first: dict,
                            rolling_window_dict_second: dict,
                            rolling_window_dict_third: dict,
                            ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :param rolling_window_dict_third:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    # CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["national_id_card", "access_method_num", "subscription_identifier", "start_of_week"]

    import logging
    logging.info("SALES L4 Input Count -->> {0}".format(str(input_df.count())))
    # metadata = CNTX.catalog.load("util_audit_metadata_table")
    # max_date = metadata.filter(f.col("table_name") == "l4_sales_number_and_volume_transaction_weekly") \
    #     .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
    #     .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
    #     .collect()[0].max_date

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    logging.info("SALES L4 rolling_df_first Count -->> {0}".format(str(rolling_df_first.count())))
    # rolling_df_first = rolling_df_first.filter(f.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_first", rolling_df_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    logging.info("SALES L4 rolling_df_second Count -->> {0}".format(str(rolling_df_second.count())))
    # rolling_df_second = rolling_df_second.filter(f.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_second", rolling_df_second)

    rolling_df_third = l4_rolling_window(input_df, rolling_window_dict_third)
    logging.info("SALES L4 rolling_df_third Count -->> {0}".format(str(rolling_df_third.count())))
    # rolling_df_third = rolling_df_third.filter(f.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_third", rolling_df_third)

    # rolling_df_first = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_first")
    # rolling_df_second = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_second")
    # rolling_df_third = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_third")

    # merged_df = rolling_df_first.join(rolling_df_second, on=join_key, how='outer')\
    #     .join(rolling_df_third, on=join_key, how='outer')
    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second, rolling_df_third])

    group_cols = ['access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month']
    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    logging.info("SALES L4 merged_df Count -->> {0}".format(str(merged_df.count())))

    return merged_df
