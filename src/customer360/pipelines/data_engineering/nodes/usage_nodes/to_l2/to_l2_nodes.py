from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os

conf = os.getenv("CONF", "local")


def usage_merge_all_data(l2_usage_call_relation_sum_weekly: DataFrame,
                         l2_usage_call_relation_sum_ir_weekly: DataFrame,
                         l2_usage_data_prepaid_postpaid_weekly: DataFrame,
                         l2_usage_ru_a_vas_postpaid_prepaid_weekly: DataFrame) -> DataFrame:
    """
    :param l2_usage_call_relation_sum_weekly:
    :param l2_usage_call_relation_sum_ir_weekly:
    :param l2_usage_data_prepaid_postpaid_weekly:
    :param l2_usage_ru_a_vas_postpaid_prepaid_weekly:
    :return:
    """
    join_key = ['crm_sub_id', 'start_of_week']
    final_df = l2_usage_call_relation_sum_weekly.join(l2_usage_call_relation_sum_ir_weekly, join_key, 'outer')
    final_df = final_df.join(l2_usage_data_prepaid_postpaid_weekly, join_key, 'outer')
    final_df = final_df.join(l2_usage_ru_a_vas_postpaid_prepaid_weekly, join_key, 'outer')

    return final_df


def build_usage_l2_layer(data_frame: DataFrame, dict_obj: dict) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        output_df = expansion(small_df, dict_obj)
        CNTX.catalog.save("l2_usage_postpaid_prepaid_weekly", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    return_df = expansion(return_df, dict_obj)

    return return_df
