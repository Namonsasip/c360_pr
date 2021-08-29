from pyspark.sql import SparkSession,DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_start_of_week_and_month
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
import os
from customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *
from customer360.utilities.spark_util import get_spark_session


# def pre_process_df(data_frame: DataFrame) -> [DataFrame, DataFrame]:
#     """
#
#     :param data_frame:
#     :return:
#     """
#     # below lines are to prepare channels
#
#     data_frame = data_frame.withColumnRenamed("campaign_child_code", "child_campaign_code")
#
#     #############  cut off process ma to join pre + post    #############
#
#     # ma_join_cols = ['subscription_identifier', "contact_date", 'child_campaign_code']
#     # contacts_ma_small = contacts_ma_small\
#     #     .select("subscription_identifier", "child_campaign_code", "contact_date", "channel_identifier").distinct()
#     #
#     # data_frame = data_frame.join(contacts_ma_small, ma_join_cols, how="left")
#
#     #############  cut off process ma to join pre + post    #############
#
#     data_frame = data_frame.withColumn("campaign_channel", F.coalesce(F.col("contact_channel"),
#                                                                       F.col("campaign_channel")))
#     #############  filter condition for support call center feature    #############
#     data_frame = data_frame.withColumn("status_khun_wijittra", F.expr \
#         ("case when lower(campaign_channel) not like '%phone%'   then 1 \
#         when lower(campaign_channel)  like '%phone%' and  contact_status_success_yn = 'Y' then 1 \
#                                               ELSE 0 END"))
#
#     data_frame = data_frame.filter((F.col("status_khun_wijittra")) != 0)
#
#     # Above logic ends here
#     all_count_grp_cols = ["access_method_num", "subscription_identifier", "contact_date",
#                           "campaign_type", "campaign_channel", "response"]
#
#     campaign_type = ['CSM Retention', 'Cross & Up Sell']
#
#     all_count_df = data_frame.groupBy(all_count_grp_cols).agg(F.count("subscription_identifier").alias("base_count"))
#
#     all_count_df = all_count_df.withColumn("campaign_type", F.when(F.col("campaign_type").isin(*campaign_type),
#                                                                    F.col("campaign_type")).otherwise(F.lit("others")))
#
#     # calculating at campaign type
#     total_camp_by_camp_type = ["access_method_num", "subscription_identifier"
#         , "contact_date", 'campaign_type', 'campaign_channel']
#
#     total_cam_by_cam_type = all_count_df.groupBy(total_camp_by_camp_type) \
#         .agg(F.sum("base_count").alias("campaign_total_by_campaign_type"))
#
#     total_campaign_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
#         .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y_n"))
#
#     total_campaign_y = all_count_df.filter(F.col("response").isin(['Y'])) \
#         .groupBy(total_camp_by_camp_type).agg(F.sum("base_count").alias("campaign_total_by_campaign_type_y"))
#
#     camp_type_final = total_cam_by_cam_type.join(total_campaign_y_n, total_camp_by_camp_type, how='left')
#     camp_type_final = camp_type_final.join(total_campaign_y, total_camp_by_camp_type, how='left')
#
#     # calculating at campaign channel
#
#     total_camp_by_camp_chnl_cols = ["access_method_num", "subscription_identifier", "contact_date", "campaign_channel"]
#
#     total_cam_by_cam_chnl = all_count_df.groupBy(total_camp_by_camp_chnl_cols) \
#         .agg(F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel"))
#
#     total_campaign_chnl_y_n = all_count_df.filter(F.col("response").isin(['Y', 'N'])) \
#         .groupBy(total_camp_by_camp_chnl_cols).agg(
#         F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel_y_n"))
#
#     total_campaign_chnl_y = all_count_df.filter(F.col("response").isin(['Y'])) \
#         .groupBy(total_camp_by_camp_chnl_cols).agg(
#         F.sum(F.col("base_count")).alias("campaign_total_by_campaign_channel_y"))
#
#     camp_chnl_final = total_cam_by_cam_chnl.join(total_campaign_chnl_y_n, total_camp_by_camp_chnl_cols, how='left')
#     camp_chnl_final = camp_chnl_final.join(total_campaign_chnl_y, total_camp_by_camp_chnl_cols, how='left')
#
#     # merging campaign_type and channel
#     final_df = camp_type_final.join(camp_chnl_final, total_camp_by_camp_chnl_cols, how="outer")
#
#     coalesce_cols = ['campaign_total_by_campaign_type', 'campaign_total_by_campaign_type_y_n',
#                      'campaign_total_by_campaign_type_y', 'campaign_total_by_campaign_channel',
#                      'campaign_total_by_campaign_channel_y_n', 'campaign_total_by_campaign_channel_y']
#
#     for col in coalesce_cols:
#         final_df = final_df.withColumn(col, F.coalesce(col, F.lit(0)))
#
#     campaign_channel_top_df = final_df.filter(F.col("campaign_channel").isNotNull()). \
#         groupBy(["access_method_num", "subscription_identifier", "campaign_channel", "contact_date"]) \
#         .agg(F.sum("campaign_total_by_campaign_channel_y_n").alias("campaign_total_campaign"),
#              F.sum("campaign_total_by_campaign_channel_y").alias("success_channel_camp"))
#
#     # this df is to calculate the top channel of the day.
#     campaign_channel_top_df = campaign_channel_top_df.\
#         withColumn("campaign_channel_success_ratio", F.col("success_channel_camp") / F.col( "campaign_total_campaign"))\
#         .drop("total_campaign", "success_channel_camp")
#
#     total_campaign = all_count_df.groupBy(["access_method_num", "subscription_identifier", "contact_date"]) \
#         .agg(F.sum(F.col("base_count")).alias("campaign_overall_count")
#              , F.max(F.col("contact_date")).alias("campaign_last_communication_date"))
#
#     final_df = final_df.join(total_campaign, ["access_method_num", "subscription_identifier", "contact_date"],
#                              how="left")
#     return final_df, campaign_channel_top_df

def pre_process_df(data_frame: DataFrame) -> DataFrame:
    """

    :param data_frame:
    :return:
    """
    conf = os.getenv("CONF", None)
    spark = get_spark_session()

    # df_l1_campaign_detail_daily.registerTempTable('l1_campaign_detail_daily')
    data_frame.registerTempTable('l1_campaign_detail_daily')

    final_df = spark.sql('''
    select contact_date, subscription_identifier,access_method_num, contact_channel, execute_date
      , case when lower(campaign_type) like '%cross%sell%' or lower(campaign_type) like '%up%sell%' then 'Cross & Up Sell'
         when lower(campaign_type) like '%retention%' then 'CSM Retention'
         when lower(campaign_type) like '%churn%' then 'CSM Churn'
         else 'Others' end campaign_type
      , count(subscription_identifier) as campaign_total 
      , sum(case when response in ('Y','N') then contact_success else 0 end) as campaign_total_eligible
      , sum(case when response = 'Y' then contact_success else 0 end) as campaign_total_success
      , sum(contact_success) as campaign_total_contact_success
      from l1_campaign_detail_daily
      where lower(coalesce(contact_status,'x')) <> 'unqualified'
      group by contact_date, subscription_identifier,access_method_num, contact_channel, execute_date
        ,case when lower(campaign_type) like '%cross%sell%' or lower(campaign_type) like '%up%sell%' then 'Cross & Up Sell'
         when lower(campaign_type) like '%retention%' then 'CSM Retention'
         when lower(campaign_type) like '%churn%' then 'CSM Churn'
         else 'Others' end
    ''')

    print('---------pre_process_df final_df------------')
    final_df.limit(10).show()
    # print('---------pre_process_df campaign_channel_top_df------------')
    # campaign_channel_top_df.limit(10).show()
    # print('---------pre_process_df campaign_detail_daily_df------------')
    # campaign_detail_daily_df.limit(10).show()
    # final_df = final_df.toDF()
    return final_df

def pre_channel_top_process_df(data_frame: DataFrame) -> DataFrame:
    """

    :param data_frame:
    :return:
    """
    conf = os.getenv("CONF", None)
    spark = get_spark_session()

    data_frame.registerTempTable('l1_campaign_detail_daily')

    campaign_channel_top_df = spark.sql('''
    select
      contact_date
    , subscription_identifier
    , access_method_num
    , contact_channel
    , sum(case when response in ('Y','N') then contact_success else 0 end) as campaign_total_eligible
    , sum(case when response = 'Y' then contact_success else 0 end) as campaign_total_success
    , execute_date
    from l1_campaign_detail_daily
    where lower(coalesce(contact_status,'x')) <> 'unqualified'
    group by contact_date, subscription_identifier,access_method_num, contact_channel, execute_date
    ''')

    print('---------pre_channel_top_process_df final_df------------')
    campaign_channel_top_df.limit(10).show()
    # print('---------pre_process_df campaign_channel_top_df------------')
    # campaign_channel_top_df.limit(10).show()
    # print('---------pre_process_df campaign_detail_daily_df------------')
    # campaign_detail_daily_df.limit(10).show()
    # final_df = final_df.toDF()
    return campaign_channel_top_df

def pre_detail_process_df(data_frame: DataFrame) -> DataFrame:
    """

    :param data_frame:
    :return:
    """
    conf = os.getenv("CONF", None)
    spark = get_spark_session()

    data_frame.registerTempTable('l1_campaign_detail_daily')

    campaign_detail_daily_df = spark.sql('''
        select
        subscription_identifier
        ,campaign_system
        ,campaign_type
        ,campaign_parent_code
        ,campaign_parent_name
        ,child_campaign_code
        ,contact_month
        ,contact_date
        ,contact_control_group
        ,contact_channel
        ,response
        ,response_type
        ,response_date
        ,contact_status
        ,contact_status_success_yn
        ,call_outcome
        ,call_attempts
        ,contact_status_last_upd
        ,current_campaign_owner
        ,update_date
        ,CASE WHEN campaign_name is null THEN child_campaign_code ELSE campaign_name END as campaign_name
        ,execute_date
        from l1_campaign_detail_daily
    ''')

    print('---------pre_detail_process_df final_df------------')
    campaign_detail_daily_df.limit(10).show()
    # print('---------pre_process_df campaign_channel_top_df------------')
    # campaign_channel_top_df.limit(10).show()
    # print('---------pre_process_df campaign_detail_daily_df------------')
    # campaign_detail_daily_df.limit(10).show()
    # final_df = final_df.toDF()
    return campaign_detail_daily_df

# def massive_processing(post_paid: DataFrame,
#                        prepaid: DataFrame,
#                        cust_prof: DataFrame,
#                        dict_1: dict,
#                        dict_2: dict) -> [DataFrame, DataFrame]:
#     """
#     :param post_paid:
#     :param prepaid:
#     :param cust_prof:
#     :param dict_1:
#     :param dict_2:
#     :return:
#     """
#     # data_set_1, data_set_2
#     unioned_df = union_dataframes_with_missing_cols(post_paid, prepaid)
#     unioned_df = add_start_of_week_and_month(input_df=unioned_df, date_column='contact_date') \
#         .withColumnRenamed("mobile_no", "access_method_num") \
#         .drop("subscription_identifier")
#     # This is recently added by K.Wijitra request
#
#     unioned_df = unioned_df.filter(F.lower(F.col("contact_status")) != 'unqualified')
#
#     joined = cust_prof.select("event_partition_date", "access_method_num", "subscription_identifier",
#                               "start_of_week", "start_of_month") \
#         .join(unioned_df, ["access_method_num", "event_partition_date", "start_of_week", "start_of_month"])
#
#     joined = joined.drop("event_partition_date", "start_of_week", "start_of_month")
#     output_df_1, output_df_2 = pre_process_df(joined)
#
#     output_df_1 = node_from_config(output_df_1, dict_1)
#     output_df_2 = node_from_config(output_df_2, dict_2)
#
#     return [output_df_1, output_df_2]

def massive_processing(postpaid: DataFrame,
                           prepaid: DataFrame,
                           fbb: DataFrame,
                           dict_1: dict,
                           dict_2: dict,
                           dict_3: dict) -> [DataFrame, DataFrame, DataFrame]:
    """
    :param post_paid:
    :param prepaid:
    :param cust_prof:
    :param dict_1:
    :param dict_2:
    :return:
    """
    # print('-----dict_1-----')
    # print(dict_1)
    # print('-----dict_2-----')
    # print(dict_2)
    # print('-----dict_3-----')
    # print(dict_3)

    conf = os.getenv("CONF", None)
    spark = get_spark_session()

    if 'current_campaign_owner' not in postpaid.columns:
        postpaid = postpaid.withColumn("current_campaign_owner", F.lit(None))
    if 'system_campaign_owner' not in postpaid.columns:
        postpaid = postpaid.withColumn("system_campaign_owner", F.lit(None))
    if 'contact_status_last_upd' not in postpaid.columns:
        postpaid = postpaid.withColumn("contact_status_last_upd", F.lit(None))
    if 'current_campaign_owner' not in prepaid.columns:
        prepaid = prepaid.withColumn("current_campaign_owner", F.lit(None))
    if 'system_campaign_owner' not in prepaid.columns:
        prepaid = prepaid.withColumn("system_campaign_owner", F.lit(None))
    if 'contact_status_last_upd' not in prepaid.columns:
        prepaid = prepaid.withColumn("contact_status_last_upd", F.lit(None))
    if 'current_campaign_owner' not in fbb.columns:
        fbb = fbb.withColumn("current_campaign_owner", F.lit(None))
    if 'system_campaign_owner' not in fbb.columns:
        fbb = fbb.withColumn("system_campaign_owner", F.lit(None))
    if 'contact_status_last_upd' not in fbb.columns:
        fbb = fbb.withColumn("contact_status_last_upd", F.lit(None))
    if 'valuesegment' not in fbb.columns:
        fbb = fbb.withColumn("valuesegment", F.lit(None))
    if 'valuesubsegment' not in fbb.columns:
        fbb = fbb.withColumn("valuesubsegment", F.lit(None))
    if 'campaign_group' not in fbb.columns:
        fbb = fbb.withColumn("campaign_group", F.lit(None))
    if 'campaign_category' not in fbb.columns:
        fbb = fbb.withColumn("campaign_category", F.lit(None))

    postpaid = postpaid.withColumn("execute_date", F.current_date())
    prepaid = prepaid.withColumn("execute_date", F.current_date())
    fbb = fbb.withColumn("execute_date", F.current_date())

    postpaid.createOrReplaceTempView("df_contact_list_post")
    prepaid.createOrReplaceTempView("df_contact_list_pre")
    fbb.createOrReplaceTempView("df_contact_list_fbb")
    min_contact_date = spark.sql('''
    select date_sub(max(to_date(cast(min_partition_date as string), 'yyyyMMdd')),1) min_contact_date
    from (
      select min(partition_date) min_partition_date
      from df_contact_list_post
      union 
      select min(partition_date) min_partition_date
      from df_contact_list_pre
      union 
      select min(partition_date) min_partition_date
      from df_contact_list_fbb
    ) all_df
    ''')
    min_contact_date.registerTempTable('min_contact_date')
    print('---------min_contact_date------------')
    min_contact_date.limit(10).show()

    df_contact_list = spark.sql('''
    select campaign_system , subscription_identifier , mobile_no, register_date , campaign_type
    , campaign_status , campaign_parent_code , campaign_child_code , campaign_name , contact_month
    , contact_date , contact_control_group , response , campaign_parent_name , campaign_channel
    , contact_status , contact_status_success_yn ,  current_campaign_owner ,  system_campaign_owner , response_type
    , call_outcome , response_date , call_attempts , contact_channel , update_date
    ,  contact_status_last_upd, valuesegment , valuesubsegment , campaign_group , campaign_category, execute_date
    , partition_date
    from df_contact_list_post a   
      join min_contact_date b
      where to_date(a.contact_date) >= b.min_contact_date
    union all
    select campaign_system , mobile_no||"-"||date_format(register_date,'yyyyMMdd') as subscription_identifier , mobile_no, register_date , campaign_type
    , campaign_status , campaign_parent_code , campaign_child_code , campaign_name , contact_month
    , contact_date , contact_control_group , response , campaign_parent_name , campaign_channel
    , contact_status , contact_status_success_yn , current_campaign_owner , system_campaign_owner , response_type
    , call_outcome , response_date , call_attempts , contact_channel , update_date
    ,  contact_status_last_upd, valuesegment , valuesubsegment , campaign_group , campaign_category, execute_date
    , partition_date
    from df_contact_list_pre a   
      join min_contact_date b
      where to_date(a.contact_date) >= b.min_contact_date
    union all
    select case when campaign_system = 'FBB' then 'Batch' else campaign_system end as campaign_system 
    , subscription_identifier , fbb_mobile_no as mobile_no, register_date , campaign_type
    , campaign_status , campaign_parent_code , campaign_child_code , campaign_name , contact_month
    , contact_date , contact_control_group , response , campaign_parent_name , campaign_channel
    , contact_status , contact_status_success_yn ,  current_campaign_owner ,  system_campaign_owner , response_type
    , call_outcome , response_date , call_attempts , contact_channel , update_date
    , contact_status_last_upd ,  valuesegment ,  valuesubsegment ,  campaign_group ,  campaign_category, execute_date
    , partition_date
    from df_contact_list_fbb a   
      join min_contact_date b
      where to_date(a.contact_date) >= b.min_contact_date
    ''')
    # post_paid.registerTempTable('campaign_tracking_post')
    # post_paid.persist()
    # display(post_paid)
    # output_df_1, output_df_2 = pre_process_df(joined)
    # print('---------clear duplicate data and filter data df_contact_list------------')
    # df_contact_list.limit(10).show()

    df_contact_list.registerTempTable('df_contact_list')

    df_l1_contact_list = spark.sql('''
        select campaign_system , subscription_identifier , mobile_no as access_method_num, register_date , campaign_type
        , campaign_status , campaign_parent_code , campaign_child_code as child_campaign_code, campaign_name , contact_month
        , contact_date , contact_control_group , response , campaign_parent_name , campaign_channel
        , contact_status , contact_status_success_yn , current_campaign_owner , system_campaign_owner , response_type
        , call_outcome , response_date , call_attempts , contact_channel , update_date
        , contact_status_last_upd ,  valuesegment , valuesubsegment , campaign_group , campaign_category, execute_date
        , subscription_identifier as c360_subscription_identifier
        , case when lower(campaign_channel) not like '%phone%' then 1 
               when lower(campaign_channel) like '%phone%' and contact_status_success_yn = 'Y' then 1 ELSE 0 END contact_success
        from (
          select *
          ,row_number() over(partition by contact_date, campaign_child_code, subscription_identifier, campaign_system, campaign_parent_code order by update_date desc ) as row_no
          from df_contact_list a
        ) filter_contact_date
        where row_no = 1 
        ''')
    output_df_1 = pre_process_df(df_l1_contact_list)
    output_df_2 = pre_channel_top_process_df(df_l1_contact_list)
    output_df_3 = pre_detail_process_df(df_l1_contact_list)

    # output_df_1, output_df_2, output_df_3 = pre_process_df(df_contact_list)
    output_df_1 = node_from_config(output_df_1, dict_1)
    output_df_2 = node_from_config(output_df_2, dict_2)
    output_df_3 = node_from_config(output_df_3, dict_3)
    print('---------node_from_config output_df_1------------')
    output_df_1.limit(10).show()
    print('---------node_from_config output_df_2------------')
    output_df_2.limit(10).show()
    print('---------node_from_config output_df_3------------')
    output_df_3.limit(10).show()
    # output_df_1 = output_df_1.toDF()

    return [output_df_1, output_df_2, output_df_3]


# def cam_post_channel_with_highest_conversion(postpaid: DataFrame,
#                                              prepaid: DataFrame,
#                                              cust_prof: DataFrame,
#                                              dictionary_obj: dict,
#                                              dictionary_obj_2: dict) -> [DataFrame, DataFrame]:
#     """
#     :param postpaid:
#     :param prepaid:
#     :param cust_prof:
#     :param dictionary_obj:
#     :param dictionary_obj_2:
#     :return:
#     """
#
#     ################################# Start Implementing Data availability checks ###############################
#     if check_empty_dfs([postpaid, prepaid, cust_prof]):
#         return [get_spark_empty_df(), get_spark_empty_df()]
#
#     postpaid = data_non_availability_and_missing_check(df=postpaid, grouping="daily", par_col="partition_date",
#                                                        target_table_name="l1_campaign_post_pre_daily")
#
#     prepaid = data_non_availability_and_missing_check(df=prepaid, grouping="daily", par_col="partition_date",
#                                                       target_table_name="l1_campaign_post_pre_daily")
#
#     cust_prof = data_non_availability_and_missing_check(df=cust_prof, grouping="daily", par_col="event_partition_date",
#                                                         target_table_name="l1_campaign_post_pre_daily")
#
#     # if check_empty_dfs([postpaid, prepaid, contacts_ma, cust_prof]):
#     if check_empty_dfs([postpaid, prepaid, cust_prof]):
#         return [get_spark_empty_df(), get_spark_empty_df()]
#
#     min_value = union_dataframes_with_missing_cols(
#         [
#             postpaid.select(
#                 F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
#             prepaid.select(
#                 F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
#             cust_prof.select(
#                 F.max(F.col("event_partition_date")).alias("max_date")),
#         ]
#     ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date
#
#     postpaid = postpaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
#
#     prepaid = prepaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
#
#     cust_prof = cust_prof.filter(F.col("event_partition_date") <= min_value)
#
#     ################################# End Implementing Data availability checks ###############################
#     first_df, second_df = massive_processing(postpaid, prepaid, cust_prof, dictionary_obj, dictionary_obj_2)
#
#     return [first_df, second_df]

def cam_post_channel_with_highest_conversion(postpaid: DataFrame,
                                                 prepaid: DataFrame,
                                                 fbb: DataFrame,
                                                 dictionary_obj: dict,
                                                 dictionary_obj_2: dict,
                                                 dictionary_obj_3: dict) -> [DataFrame, DataFrame, DataFrame]:
    """
    :param postpaid:
    :param prepaid:
    :param cust_prof:
    :param dictionary_obj:
    :param dictionary_obj_2:
    :return:
    """
    conf = os.getenv("CONF", None)
    spark = get_spark_session()

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([postpaid, prepaid, fbb]):
        return get_spark_empty_df()

    postpaid = data_non_availability_and_missing_check(df=postpaid, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_campaign_post_pre_daily")

    prepaid = data_non_availability_and_missing_check(df=prepaid, grouping="daily", par_col="partition_date",
                                                      target_table_name="l1_campaign_post_pre_daily")

    fbb = data_non_availability_and_missing_check(df=fbb, grouping="daily", par_col="partition_date",
                                                      target_table_name="l1_campaign_post_pre_daily")

    # cust_prof = data_non_availability_and_missing_check(df=cust_prof, grouping="daily", par_col="event_partition_date",
    #                                                     target_table_name="l1_campaign_post_pre_daily")

    # if check_empty_dfs([postpaid, prepaid, contacts_ma, cust_prof]):
    if check_empty_dfs([postpaid, prepaid, fbb]):
        return get_spark_empty_df()

    min_value = union_dataframes_with_missing_cols(
        [
            postpaid.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            prepaid.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            fbb.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            # cust_prof.select(
            #     F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    postpaid = postpaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    prepaid = prepaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    fbb = fbb.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    min_value_65 = union_dataframes_with_missing_cols(
        [
            postpaid.select(
                F.date_sub(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'), 65).alias("min_date")),
            prepaid.select(
                F.date_sub(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'), 65).alias("min_date")),
            fbb.select(
                F.date_sub(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'), 65).alias("min_date")),

        ]
    ).select(F.max(F.col("min_date")).alias("last_date")).collect()[0].last_date

    # min_value_65 = spark.createDataFrame([('20200702',)], ['min_date'])
    # min_value_65 = min_value_65.select(F.to_date(F.lit('20200702'), 'yyyyMMdd').alias('last_date')).collect()[0].last_date

    postpaid = postpaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') >= min_value_65)

    prepaid = prepaid.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') >= min_value_65)

    fbb = fbb.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') >= min_value_65)

    # cust_prof = cust_prof.filter(F.col("event_partition_date") <= min_value)

    print('---------postpaid filter max partition_date------------')
    postpaid.select(F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")).show()
    postpaid.select(F.to_date(F.min(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("min_date")).show()
    # postpaid.limit(10).show()
    # postpaid = postpaid.toDF()

    ################################# End Implementing Data availability checks ###############################
    first_df, second_df, third_df = massive_processing(postpaid, prepaid, fbb, dictionary_obj, dictionary_obj_2, dictionary_obj_3)
    # print('---------first_df output------------')
    # first_df.limit(10).show()
    # print('---------second_df output------------')
    # second_df.limit(10).show()
    # print('---------third_df output------------')
    # third_df.limit(10).show()

    return [first_df, second_df, third_df]
