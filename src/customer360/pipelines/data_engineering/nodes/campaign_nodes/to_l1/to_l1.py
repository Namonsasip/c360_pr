from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

conf = os.environ["CONF"]


def massive_processing(data_frame_1, data_frame_2, data_frame_3, dict_1
                       , dict_2, data_set_1, data_set_2) -> [DataFrame, DataFrame]:
    """
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame_1
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))

        postpaid = data_frame_1.filter(F.col("partition_date").isin(*[curr_item]))
        prepaid = data_frame_2.filter(F.col("partition_date").isin(*[curr_item]))
        fbb = data_frame_3.filter(F.col("partition_date").isin(*[curr_item]))

        unioned_df = union_dataframes_with_missing_cols(prepaid, postpaid, fbb)

        output_df_1, output_df_2 = pre_process_df(unioned_df)

        output_df_1 = node_from_config(output_df_1, dict_1)
        output_df_2 = node_from_config(output_df_2, dict_2)

        CNTX.catalog.save(data_set_1, output_df_1)
        CNTX.catalog.save(data_set_2, output_df_2)

    logging.info("Final date to run for {0}".format(str(first_item)))

    postpaid = data_frame_1.filter(F.col("partition_date").isin(*[first_item]))
    prepaid = data_frame_2.filter(F.col("partition_date").isin(*[first_item]))
    fbb = data_frame_3.filter(F.col("partition_date").isin(*[first_item]))

    unioned_df = union_dataframes_with_missing_cols(prepaid, postpaid, fbb)

    output_df_1, output_df_2 = pre_process_df(unioned_df)

    output_df_1 = node_from_config(output_df_1, dict_1)
    output_df_2 = node_from_config(output_df_2, dict_2)

    return [output_df_1, output_df_2]


def cam_post_channel_with_highest_conversion(postpaid: DataFrame,
                                             prepaid: DataFrame,
                                             fbb: DataFrame,
                                             dictionary_obj,
                                             dictionary_obj_2) -> [DataFrame, DataFrame]:
    """
    :param postpaid:
    :param prepaid:
    :param fbb
    :param dictionary_obj:
    :param dictionary_obj_2:
    :return:
    """
    first_df, second_df = massive_processing(postpaid, prepaid, fbb, dictionary_obj, dictionary_obj_2,
                                             'l1_campaign_post_pre_fbb_daily', 'l1_campaign_top_channel_daily')

    return [first_df, second_df]


def pre_process_df(data_frame: DataFrame) -> [DataFrame, DataFrame]:
    """
    :param data_frame:
    :return:
    """
    all_count_grp_cols = ['subscription_identifier', "contact_date",
                          'campaign_type', 'campaign_channel', 'response']

    campaign_type = ['CSM Retention', 'Cross & Up Sell']
    all_count_df = data_frame.groupBy(all_count_grp_cols).agg(F.count("subscription_identifier").alias("base_count"))

    all_count_df = all_count_df.withColumn("campaign_type", F.when(F.col("campaign_type").isin(*campaign_type),
                                                                   F.col("campaign_type")).otherwise(F.lit("others")))

    # calculating at campaign type
    total_camp_by_camp_type = ['subscription_identifier', "contact_date", 'campaign_type', 'campaign_channel']

    total_cam_by_cam_type = all_count_df.groupBy(total_camp_by_camp_type) \
        .agg(F.sum("base_count").alias("campaign_total_by_campaign_type"))

    total_campaign_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y_n"))

    total_campaign_y = all_count_df.filter(F.col("response").isin(['Y'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y"))

    camp_type_final = total_cam_by_cam_type.join(total_campaign_y_n, total_camp_by_camp_type, how='left')
    camp_type_final = camp_type_final.join(total_campaign_y, total_camp_by_camp_type, how='left')

    # calculating at campaign channel

    total_camp_by_camp_chnl_cols = ['subscription_identifier', "contact_date", 'campaign_channel']

    total_cam_by_cam_chnl = all_count_df.groupBy(total_camp_by_camp_chnl_cols) \
        .agg(F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel"))

    total_campaign_chnl_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
        .groupBy(total_camp_by_camp_chnl_cols).agg(
        F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel_y_n"))

    total_campaign_chnl_y = all_count_df.filter(F.col("response").isin(['Y'])) \
        .groupBy(total_camp_by_camp_chnl_cols).agg(
        F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel_y"))

    camp_chnl_final = total_cam_by_cam_chnl.join(total_campaign_chnl_y_n, total_camp_by_camp_chnl_cols, how='left')
    camp_chnl_final = camp_chnl_final.join(total_campaign_chnl_y, total_camp_by_camp_chnl_cols, how='left')

    # merging campaign_type and channel
    final_df = camp_type_final.join(camp_chnl_final, total_camp_by_camp_chnl_cols, how="outer")

    coalesce_cols = ['campaign_total_by_campaign_type', 'campaign_total_by_campaign_type_y_n',
                     'campaign_total_by_campaign_type_y', 'campaign_total_by_campaign_channel',
                     'campaign_total_by_campaign_channel_y_n', 'campaign_total_by_campaign_channel_y']

    for col in coalesce_cols:
        final_df = final_df.withColumn(col, F.coalesce(col, F.lit(0)))

    campaign_channel_top_df = final_df.filter(F.col("campaign_channel").isNotNull()). \
        groupBy(["subscription_identifier", "campaign_channel", "contact_date"]) \
        .agg(F.sum("campaign_total_by_campaign_channel_y_n").alias("campaign_total_campaign"),
             F.sum("campaign_total_by_campaign_channel_y").alias("success_channel_camp"))

    # this df is to calculate the top channel of the day.
    campaign_channel_top_df = campaign_channel_top_df.withColumn("campaign_channel_success_ratio",
                                                                 F.col("success_channel_camp") / F.col(
                                                                     "campaign_total_campaign")) \
        .drop("total_campaign", "success_channel_camp")

    total_campaign = all_count_df.groupBy(["subscription_identifier", "contact_date"]) \
        .agg(F.sum(F.col("base_count")).alias("campaign_overall_count")
             , F.max(F.col("contact_date")).alias("campaign_last_communication_date"))

    final_df = final_df.join(total_campaign, ["subscription_identifier", "contact_date"], how="left")

    return final_df, campaign_channel_top_df
