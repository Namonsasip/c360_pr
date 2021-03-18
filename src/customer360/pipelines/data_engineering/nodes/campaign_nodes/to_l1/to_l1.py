from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_start_of_week_and_month
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
import os
from customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *

conf = os.getenv("CONF", None)


def pre_process_df(data_frame: DataFrame) -> [DataFrame, DataFrame]:
    """

    :param data_frame:
    :return:
    """
    # below lines are to prepare channels

    data_frame = data_frame.withColumnRenamed("campaign_child_code", "child_campaign_code")

    #############  cut off process ma to join pre + post    #############

    # ma_join_cols = ['subscription_identifier', "contact_date", 'child_campaign_code']
    # contacts_ma_small = contacts_ma_small\
    #     .select("subscription_identifier", "child_campaign_code", "contact_date", "channel_identifier").distinct()
    #
    # data_frame = data_frame.join(contacts_ma_small, ma_join_cols, how="left")

    #############  cut off process ma to join pre + post    #############

    data_frame = data_frame.withColumn("campaign_channel", F.coalesce(F.col("contact_channel"),
                                                                      F.col("campaign_channel")))
    #############  filter condition for support call center feature    #############
    data_frame = data_frame.withColumn("status_khun_wijittra", F.expr \
        ("case when lower(campaign_channel) not like '%phone%'   then 1 \
        when lower(campaign_channel)  like '%phone%' and  contact_status_success_yn = 'Y' then 1 \
                                              ELSE 0 END"))

    data_frame = data_frame.filter((F.col("status_khun_wijittra")) != 0)

    # Above logic ends here
    all_count_grp_cols = ["access_method_num", "subscription_identifier", "contact_date",
                          "campaign_type", "campaign_channel", "response"]

    campaign_type = ['CSM Retention', 'Cross & Up Sell']

    all_count_df = data_frame.groupBy(all_count_grp_cols).agg(F.count("subscription_identifier").alias("base_count"))

    all_count_df = all_count_df.withColumn("campaign_type", F.when(F.col("campaign_type").isin(*campaign_type),
                                                                   F.col("campaign_type")).otherwise(F.lit("others")))

    # calculating at campaign type
    total_camp_by_camp_type = ["access_method_num", "subscription_identifier"
        , "contact_date", 'campaign_type', 'campaign_channel']

    total_cam_by_cam_type = all_count_df.groupBy(total_camp_by_camp_type) \
        .agg(F.sum("base_count").alias("campaign_total_by_campaign_type"))

    total_campaign_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y_n"))

    total_campaign_y = all_count_df.filter(F.col("response").isin(['Y'])) \
        .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y"))

    camp_type_final = total_cam_by_cam_type.join(total_campaign_y_n, total_camp_by_camp_type, how='left')
    camp_type_final = camp_type_final.join(total_campaign_y, total_camp_by_camp_type, how='left')

    # calculating at campaign channel

    total_camp_by_camp_chnl_cols = ["access_method_num", "subscription_identifier", "contact_date", "campaign_channel"]

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
        groupBy(["access_method_num", "subscription_identifier", "campaign_channel", "contact_date"]) \
        .agg(F.sum("campaign_total_by_campaign_channel_y_n").alias("campaign_total_campaign"),
             F.sum("campaign_total_by_campaign_channel_y").alias("success_channel_camp"))

    # this df is to calculate the top channel of the day.
    campaign_channel_top_df = campaign_channel_top_df.\
        withColumn("campaign_channel_success_ratio", F.col("success_channel_camp") / F.col( "campaign_total_campaign"))\
        .drop("total_campaign", "success_channel_camp")

    total_campaign = all_count_df.groupBy(["access_method_num", "subscription_identifier", "contact_date"]) \
        .agg(F.sum(F.col("base_count")).alias("campaign_overall_count")
             , F.max(F.col("contact_date")).alias("campaign_last_communication_date"))

    final_df = final_df.join(total_campaign, ["access_method_num", "subscription_identifier", "contact_date"],
                             how="left")
    return final_df, campaign_channel_top_df


def massive_processing(post_paid: DataFrame,
                       prepaid: DataFrame,
                       cust_prof: DataFrame,
                       dict_1: dict,
                       dict_2: dict) -> [DataFrame, DataFrame]:
    """
    :param post_paid:
    :param prepaid:
    :param cust_prof:
    :param dict_1:
    :param dict_2:
    :return:
    """
    # data_set_1, data_set_2
    unioned_df = union_dataframes_with_missing_cols(post_paid, prepaid)
    unioned_df = add_start_of_week_and_month(input_df=unioned_df, date_column='contact_date') \
        .withColumnRenamed("mobile_no", "access_method_num") \
        .drop("subscription_identifier")
    # This is recently added by K.Wijitra request

    unioned_df = unioned_df.filter(F.lower(F.col("contact_status")) != 'unqualified')

    joined = cust_prof.select("event_partition_date", "access_method_num", "subscription_identifier",
                              "start_of_week", "start_of_month") \
        .join(unioned_df, ["access_method_num", "event_partition_date", "start_of_week", "start_of_month"])

    joined = joined.drop("event_partition_date", "start_of_week", "start_of_month")
    output_df_1, output_df_2 = pre_process_df(joined)

    output_df_1 = node_from_config(output_df_1, dict_1)
    output_df_2 = node_from_config(output_df_2, dict_2)

    return [output_df_1, output_df_2]


def cam_post_channel_with_highest_conversion(postpaid: DataFrame,
                                             prepaid: DataFrame,
                                             cust_prof: DataFrame,
                                             dictionary_obj: dict,
                                             dictionary_obj_2: dict) -> [DataFrame, DataFrame]:
    """
    :param postpaid:
    :param prepaid:
    :param cust_prof:
    :param dictionary_obj:
    :param dictionary_obj_2:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([postpaid, prepaid, cust_prof]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    postpaid = data_non_availability_and_missing_check(df=postpaid, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_campaign_post_pre_daily")

    prepaid = data_non_availability_and_missing_check(df=prepaid, grouping="daily", par_col="partition_date",
                                                      target_table_name="l1_campaign_post_pre_daily")

    cust_prof = data_non_availability_and_missing_check(df=cust_prof, grouping="daily", par_col="event_partition_date",
                                                        target_table_name="l1_campaign_post_pre_daily")

    # if check_empty_dfs([postpaid, prepaid, contacts_ma, cust_prof]):
    if check_empty_dfs([postpaid, prepaid, cust_prof]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            postpaid.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            prepaid.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            cust_prof.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    postpaid = postpaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    prepaid = prepaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    cust_prof = cust_prof.filter(F.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################
    first_df, second_df = massive_processing(postpaid, prepaid, cust_prof, dictionary_obj, dictionary_obj_2)

    return [first_df, second_df]


def test_sampling(df_input: DataFrame) -> [DataFrame, DataFrame]:

    if check_empty_dfs([df_input]):
        return [get_spark_empty_df()]

    df_input = add_event_week_and_month_from_yyyymmdd(input_df=df_input, date_column='partition_date')

    #df_output = node_from_config(df_input, dictionary_obj)

    return df_input

