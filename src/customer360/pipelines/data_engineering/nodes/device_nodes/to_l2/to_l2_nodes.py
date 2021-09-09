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
    # if check_empty_dfs([hs_summary, hs_configs]):
    #     return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    hs_configs = hs_configs.withColumn("partition_date", hs_configs["partition_date"].cast(StringType()))
    hs_configs = hs_configs.withColumn("start_of_week",
                                       f.to_date(f.date_trunc('week', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))

    run_date_in_pipeline = str(os.environ["RUN_DATE"])
    # hs_summary = hs_summary.filter(F.col('start_of_week').between('2021-08-02', '2021-08-09'))
    # hs_configs = hs_configs.filter(F.col('start_of_week').between('2021-08-02', '2021-08-09'))
    hs_summary = hs_summary.filter(F.col('start_of_week') == run_date_in_pipeline)
    hs_configs = hs_configs.filter(F.col('start_of_week') == run_date_in_pipeline)
    logging.info("---------------- Filter "+run_date_in_pipeline+" Completed ----------------")


    hs_config_sel = ["start_of_week", "hs_brand_code", "hs_model_code", "month_id", "os", "launchprice", "saleprice",
                     "gprs_handset_support", "hsdpa", "google_map", "video_call"]
    hs_configs = hs_configs.select(hs_config_sel)
    partition = Window.partitionBy(["start_of_week", "hs_brand_code", "hs_model_code"]).orderBy(
        F.col("month_id").desc())
    # removing duplicates within a week
    hs_configs = hs_configs.withColumn("rnk", F.row_number().over(partition))
    hs_configs = hs_configs.filter(f.col("rnk") == 1)
    logging.info("---------------- Cal Rank ----------------")

    # min_value = union_dataframes_with_missing_cols(
    #     [
    #         hs_summary.select(
    #             F.max(F.col("start_of_week")).alias("max_date")),
    #         hs_configs.select(
    #             F.max(F.col("start_of_week")).alias("max_date")),
    #     ]
    # ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date
    # logging.info("---------------- Cal min value ----------------")
    # hs_summary = hs_summary.filter(F.col("start_of_week") <= min_value)
    # hs_configs = hs_configs.filter(F.col("start_of_week") <= min_value)

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_week == hs_configs.start_of_week), "left") \
        .drop(hs_configs.start_of_week)

    return joined_data




