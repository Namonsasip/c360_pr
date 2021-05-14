from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def build_usage_l3_layer(data_frame: DataFrame, dict_obj: dict) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([data_frame]):
        return get_spark_empty_df()

    data_frame = data_non_availability_and_missing_check(df=data_frame, grouping="monthly",
                                                         par_col="event_partition_date",
                                                         target_table_name="l3_usage_postpaid_prepaid_monthly",
                                                         missing_data_check_flg='Y')

    if check_empty_dfs([data_frame]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        output_df = expansion(small_df, dict_obj)
        CNTX.catalog.save("l3_usage_postpaid_prepaid_monthly", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    return_df = expansion(return_df, dict_obj)

    return return_df
