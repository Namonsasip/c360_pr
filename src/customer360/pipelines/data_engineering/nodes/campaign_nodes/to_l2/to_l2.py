from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os

conf = os.environ["CONF"]


def build_campaign_l2_layer(l1_campaign_post_pre_fbb_daily: DataFrame,
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

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = l1_campaign_post_pre_fbb_daily
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        top_campaign_df = l1_campaign_top_channel_daily.filter(F.col("start_of_week").isin(*[curr_item]))
        output_df_1 = expansion(small_df, dictObj_1)
        output_df_2 = expansion(top_campaign_df, dictObj_2)
        CNTX.catalog.save("l2_campaign_postpaid_prepaid_fbb_weekly", output_df_1)
        CNTX.catalog.save("l1_campaign_top_channel_weekly", output_df_2)

    small_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    top_campaign_df = l1_campaign_top_channel_daily.filter(F.col("start_of_week").isin(*[first_item]))
    first_return_df = expansion(small_df, dictObj_1)
    second_return_df = expansion(top_campaign_df, dictObj_2)

    return [first_return_df, second_return_df]
