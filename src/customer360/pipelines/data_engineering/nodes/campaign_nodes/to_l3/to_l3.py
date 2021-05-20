import logging
import os
from pathlib import Path

from kedro.context.context import load_context
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.config_parser import expansion
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def build_campaign_l3_layer(l1_campaign_post_pre_fbb_daily: DataFrame,
                            l1_campaign_top_channel_daily: DataFrame,
                            dictObj_1: dict,
                            dictObj_2: dict) -> [DataFrame, DataFrame]:
    """

    :param l1_campaign_post_pre_fbb_daily:
    :param l1_campaign_top_channel_daily:
    :param dictObj_1:
    :param dictObj_2:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([l1_campaign_post_pre_fbb_daily, l1_campaign_top_channel_daily]):
        return [get_spark_empty_df(), get_spark_empty_df()]


    l1_campaign_post_pre_fbb_daily = data_non_availability_and_missing_check(
        df=l1_campaign_post_pre_fbb_daily,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_campaign_postpaid_prepaid_monthly")
        # missing_data_check_flg='Y')


    l1_campaign_top_channel_daily = data_non_availability_and_missing_check(
        df=l1_campaign_top_channel_daily,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_campaign_top_channel_monthly")
        # missing_data_check_flg='Y')



    if check_empty_dfs([l1_campaign_post_pre_fbb_daily, l1_campaign_top_channel_daily]):
        return [get_spark_empty_df(), get_spark_empty_df()]


    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = l1_campaign_post_pre_fbb_daily
    data_frame = data_frame.drop('run_date')
    print('dropdropdropdrop')
    data_frame = data_frame.withColumn("run_date", F.current_date())
    print('***************************************************************')
    data_frame.show()
    print('***************************************************************')
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        top_campaign_df = l1_campaign_top_channel_daily.filter(F.col("start_of_month").isin(*[curr_item]))
        top_campaign_df = top_campaign_df.drop('run_date')
        top_campaign_df = top_campaign_df.withColumn("run_date", F.current_date())
        print('** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** * ')
        top_campaign_df.show()
        print('** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** *')
        output_df_1 = expansion(small_df, dictObj_1)
        output_df_2 = expansion(top_campaign_df, dictObj_2)
        CNTX.catalog.save("l3_campaign_postpaid_prepaid_monthly", output_df_1)
        CNTX.catalog.save("l3_campaign_top_channel_monthly", output_df_2)

    small_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    top_campaign_df = l1_campaign_top_channel_daily.filter(F.col("start_of_month").isin(*[first_item]))
    first_return_df = expansion(small_df, dictObj_1)
    second_return_df = expansion(top_campaign_df, dictObj_2)



    return [first_return_df, second_return_df]
