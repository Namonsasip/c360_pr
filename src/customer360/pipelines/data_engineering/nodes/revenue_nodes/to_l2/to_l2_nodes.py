from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check

conf = os.getenv("CONF", None)


def build_revenue_l2_layer(data_frame: DataFrame, dict_obj: dict, exception_partition=None) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([data_frame]):
        return data_frame

    data_frame = data_non_availability_and_missing_check(df=data_frame, grouping="weekly",
                                                         par_col="event_partition_date",
                                                         target_table_name="l2_revenue_prepaid_pru_f_usage_multi_weekly",
                                                         missing_data_check_flg='Y',
                                                         exception_partitions=exception_partition)

    if check_empty_dfs([data_frame]):
        return data_frame

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        output_df = expansion(small_df, dict_obj)
        CNTX.catalog.save("l2_revenue_prepaid_pru_f_usage_multi_weekly", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    return_df = expansion(return_df, dict_obj)

    return return_df
