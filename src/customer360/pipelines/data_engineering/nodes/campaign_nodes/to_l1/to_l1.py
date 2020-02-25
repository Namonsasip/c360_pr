from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from customer360.utilities.config_parser import node_from_config


def get_spark_session():
    return SparkSession.builder.getOrCreate()


def cam_post_channel_with_highest_conversion(data_frame: DataFrame, dictionary_obj) -> [DataFrame, DataFrame]:
    """
    :param data_frame:
    :return:
    """
    data_frame = data_frame.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.to_date("contact_date")))) \
        .withColumn("event_partition_date", F.to_date("contact_date"))

    all_count_grp_cols = ['subscription_identifier', "event_partition_date", "start_of_week",
                          'campaign_type', 'campaign_channel', 'response']

    all_count_df = data_frame.groupBy(all_count_grp_cols).agg(F.count("subscription_identifier").alias("base_count"))

    # calculating at campaign type
    total_camp_by_camp_type = ['subscription_identifier', "event_partition_date",
                               "start_of_week", 'campaign_type', 'campaign_channel']

    total_cam_by_cam_type = all_count_df.groupBy(total_camp_by_camp_type) \
        .agg(F.sum("base_count").alias("campaign_total_campaign_by_campaign_type"))

    total_campaign_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_campaign_by_campaign_type_y_n"))

    total_campaign_y = all_count_df.filter(F.col("response").isin(['Y'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_campaign_by_campaign_type_y"))

    camp_type_final = total_cam_by_cam_type.join(total_campaign_y_n, total_camp_by_camp_type, how='left')
    camp_type_final = camp_type_final.join(total_campaign_y, total_camp_by_camp_type, how='left')

    # calculating at campaign channel

    total_camp_by_camp_chnl_cols = ['subscription_identifier', "event_partition_date",
                                    "start_of_week", 'campaign_channel']

    total_cam_by_cam_chnl = all_count_df.groupBy(total_camp_by_camp_chnl_cols) \
        .agg(F.sum(F.col("base_count")).alias("campaign_total_campaign_by_campaign_channel"))

    total_campaign_chnl_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
        .groupBy(total_camp_by_camp_chnl_cols).agg(
        F.sum(F.col("base_count")).alias("campaign_total_campaign_by_campaign_channel_y_n"))

    total_campaign_chnl_y = all_count_df.filter(F.col("response").isin(['Y'])) \
        .groupBy(total_camp_by_camp_chnl_cols).agg(
        F.sum(F.col("base_count")).alias("campaign_total_campaign_by_campaign_channel_y"))

    camp_chnl_final = total_cam_by_cam_chnl.join(total_campaign_chnl_y_n, total_camp_by_camp_chnl_cols, how='left')
    camp_chnl_final = camp_chnl_final.join(total_campaign_chnl_y, total_camp_by_camp_chnl_cols, how='left')

    # merging campaign_type and channel
    final_df = camp_type_final.join(camp_chnl_final, total_camp_by_camp_chnl_cols, how="outer")

    coalesce_cols = ['campaign_total_campaign_by_campaign_type', 'campaign_total_campaign_by_campaign_type_y_n',
                     'campaign_total_campaign_by_campaign_type_y', 'campaign_total_campaign_by_campaign_channel',
                     'campaign_total_campaign_by_campaign_channel_y_n', 'campaign_total_campaign_by_campaign_channel_y']

    for col in coalesce_cols:
        final_df = final_df.withColumn(col, F.coalesce(col, F.lit(0)))

    campaign_channel_top_df = final_df.filter(F.col("campaign_channel").isNotNull()). \
        groupBy(["subscription_identifier", "campaign_channel", "event_partition_date", "start_of_week"]) \
        .agg(F.sum("campaign_total_campaign_by_campaign_channel_y_n").alias("campaign_total_campaign"),
             F.sum("campaign_total_campaign_by_campaign_channel_y").alias("success_channel_camp"))

    # this df is to calculate the top channel of the day.
    campaign_channel_top_df = campaign_channel_top_df.withColumn("campaign_channel_success_ratio",
                                                                 F.col("success_channel_camp") / F.col(
                                                                     "campaign_total_campaign")) \
        .drop("total_campaign", "success_channel_camp")

    total_campaign = all_count_df.groupBy(["subscription_identifier", "event_partition_date", "start_of_week"]) \
        .agg(F.sum(F.col("base_count")).alias("campaign_overall_count")
             , F.max(F.col("event_partition_date")).alias("campaign_last_communication_date"))

    final_df = final_df.join(total_campaign, ["subscription_identifier", "event_partition_date", "start_of_week"],
                             how="left")

    final_df = node_from_config(final_df, dictionary_obj)

    return [final_df, campaign_channel_top_df]
