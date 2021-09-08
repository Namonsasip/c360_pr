import logging, os
from pathlib import Path
from kedro.context import load_context

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df


def device_summary_with_configuration(hs_summary: DataFrame,
                                      hs_configs: DataFrame,
                                      exception_partitions_hs_summary: list) -> DataFrame:
    """
    :param hs_summary:
    :param hs_configs:
    :param exception_partitions_hs_summary:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([hs_summary, hs_configs]):
        return get_spark_empty_df()

    # hs_summary = data_non_availability_and_missing_check(df=hs_summary, grouping="weekly",
    #                                                      par_col="event_partition_date",
    #                                                      target_table_name="l2_device_summary_with_config_weekly",
    #                                                      missing_data_check_flg='Y',
    #                                                      exception_partitions=exception_partitions_hs_summary)
    #
    # hs_configs = data_non_availability_and_missing_check(df=hs_configs, grouping="weekly", par_col="partition_date",
    #                                                      target_table_name="l2_device_summary_with_config_weekly")

    if check_empty_dfs([hs_summary, hs_configs]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    hs_configs = hs_configs.withColumn("partition_date", hs_configs["partition_date"].cast(StringType()))
    hs_configs = hs_configs.withColumn("start_of_week",
                                       f.to_date(f.date_trunc('week', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))

    hs_config_sel = ["start_of_week", "hs_brand_code", "hs_model_code", "month_id", "os", "launchprice", "saleprice",
                     "gprs_handset_support", "hsdpa", "google_map", "video_call"]

    # filter start of week for rerun process
    hs_summary = hs_summary.filter(F.col('start_of_week') == '2021-06-21')
    hs_configs = hs_configs.filter(F.col('start_of_week') == '2021-06-21')
    logging.info("---------------- Filter Completed ----------------")

    hs_configs = hs_configs.select(hs_config_sel)

    partition = Window.partitionBy(["start_of_week", "hs_brand_code", "hs_model_code"]).orderBy(
        F.col("month_id").desc())



    # removing duplicates within a week
    hs_configs = hs_configs.withColumn("rnk", F.row_number().over(partition))
    hs_configs = hs_configs.filter(f.col("rnk") == 1)

    # min_value = union_dataframes_with_missing_cols(
    #     [
    #         hs_summary.select(
    #             F.max(F.col("start_of_week")).alias("max_date")),
    #         hs_configs.select(
    #             F.max(F.col("start_of_week")).alias("max_date")),
    #     ]
    # ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date
    #
    # hs_summary = hs_summary.filter(F.col("start_of_week") <= min_value)
    # hs_configs = hs_configs.filter(F.col("start_of_week") <= min_value)

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_week == hs_configs.start_of_week), "left") \
        .drop(hs_configs.start_of_week)
    return joined_data


conf = os.getenv("CONF", "base")


def massive_device_node_from_config(input_df: DataFrame, config: dict):
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_array = list(divide_chunks(mvv_array, 4))
    add_list = mvv_array
    first_item = add_list[-1]
    add_list.remove(first_item)

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        output_df = node_from_config(small_df, config)
        CNTX.catalog.save("l2_device_summary_with_config_weekly", output_df)
    logging.info("running for dates {0}".format(str(first_item)))
    final_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    return_df = node_from_config(final_df, config)

    return return_df


