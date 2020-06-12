from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from pyspark.sql import DataFrame, functions as f
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.config_parser import node_from_config

conf = os.getenv("CONF", None)


def run_for_complaints_to_l2_pipeline_from_l1(input_df: DataFrame,
                                              target_table_name: str,
                                              node_from_config_dict: dict
                                              ):
    """
    :param input_df:
    :param target_table_name:
    :param node_from_config_dict:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name=target_table_name,
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("start_of_week").isin(*[curr_item]))
        output_df = node_from_config(small_df, node_from_config_dict)
        CNTX.catalog.save(target_table_name, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col("start_of_week").isin(*[first_item]))
    return_df = node_from_config(return_df, node_from_config_dict)

    return return_df


