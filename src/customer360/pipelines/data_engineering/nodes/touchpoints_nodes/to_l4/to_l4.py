from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, get_spark_empty_df,\
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql
import os
from pathlib import Path
from kedro.context.context import load_context


conf = os.getenv("CONF", None)


def build_touchpoints_weekly_features(input_df: DataFrame,
                                      first_dict: dict,
                                      second_dict: dict) -> DataFrame:
    """
    :param input_df:
    :param first__dict:
    :param second_dict:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    # CNTX = load_context(Path.cwd(), env=conf)
    #
    # metadata = CNTX.catalog.load("util_audit_metadata_table")
    # max_date = metadata.filter(F.col("table_name") == "l4_touchpoints_nim_work_features") \
    #     .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
    #     .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
    #     .collect()[0].max_date

    input_df = input_df.cache()
    first_df = l4_rolling_window(input_df, first_dict)
    #first_df = first_df.filter(F.col("start_of_week") > max_date)
    #CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_first_first", first_first_df)

    second_df = l4_rolling_window(input_df, second_dict)
    #second_df = second_df.filter(F.col("start_of_week") > max_date)
    #CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_first_second", first_second_df)

    # second_first_df = l4_rolling_window(input_df, second_first_dict)
    # second_first_df = second_first_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_second_first", second_first_df)

    #first_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_first_first")
    #first_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_first_second")
    #second_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_second_first")
    group_cols = ["subscription_identifier", "start_of_week"]

    merged_df = union_dataframes_with_missing_cols(first_df, second_df)
    sql_query = gen_max_sql(merged_df, "test_table", group_cols)

    return_df = execute_sql(merged_df, "test_table", sql_query)
    return return_df
