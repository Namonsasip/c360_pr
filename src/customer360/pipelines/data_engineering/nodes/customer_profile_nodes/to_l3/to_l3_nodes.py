from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, union_dataframes_with_missing_cols
from kedro.context.context import load_context
from pathlib import Path
import os, logging
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.functions import col
conf = os.getenv("CONF", None)


def df_copy_for_l3_customer_profile_include_1mo_non_active(input_df, segment_df, multisum_df):
    ################################ Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, segment_df, multisum_df]):
        return get_spark_empty_df(3)

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_customer_profile_include_1mo_non_active")

    segment_df = data_non_availability_and_missing_check(df=segment_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_customer_profile_include_1mo_non_active")

    multisum_df = data_non_availability_and_missing_check(df=multisum_df, grouping="monthly",
                                                       par_col="start_of_month",
                                                       target_table_name="l3_customer_profile_include_1mo_non_active")

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(f.max(f.col("partition_month")).alias("max_date")),
            segment_df.select(f.max(f.col("partition_month")).alias("max_date")),
            multisum_df.select(f.max(f.col("partition_month")).alias("max_date"))
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("partition_month") <= min_value)
    segment_df = segment_df.filter(f.col("partition_month") <= min_value)
    multisum_df = multisum_df.filter(f.col("partition_month") <= min_value)

    if check_empty_dfs([input_df, segment_df, multisum_df]):
        return get_spark_empty_df(3)

    ################################ End Implementing Data availability checks ###############################

    # input_df = input_df.where("partition_month = '202103'")

    return [input_df, segment_df, multisum_df]


def df_copy_for_l3_customer_profile_billing_level_features(input_df):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_customer_profile_billing_level_features")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def df_copy_for_l3_customer_profile_billing_level_volume_of_active_contracts(input_df):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_customer_profile_billing_level_volume_of_active_contracts")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def add_last_month_inactive_user(input_df):
    if check_empty_dfs([input_df]):
        return input_df

    input_df.createOrReplaceTempView("input_df")
    spark = get_spark_session()

    inactive_cust_feature_list = []
    normal_feature_list = []
    for each_feature in input_df.columns:
        if each_feature == 'partition_month':
            # To forward inactive customer to next month
            inactive_cust_feature_list.append("df1.next_month as partition_month")
            normal_feature_list.append(each_feature)
            continue

        if each_feature == 'last_month' or each_feature == 'next_month':
            continue

        if each_feature == 'cust_active_this_month':
            inactive_cust_feature_list.append("'N' as cust_active_this_month")
            normal_feature_list.append(each_feature)
            continue

        inactive_cust_feature_list.append("df1.{feature_name} as {feature_name}"
                                          .format(feature_name=each_feature))
        normal_feature_list.append(each_feature)

    df = spark.sql("""
        with non_active_customer as (
            select {inactive_cust_feature_list}
            from (
                select * from input_df 
                where partition_month != (select max(partition_month) from input_df)
                    and charge_type = 'Pre-paid'
            ) df1
            left anti join input_df df2
            on df1.partition_month = df2.last_month
                    and df1.access_method_num = df2.access_method_num
                    and df1.register_date = df2.register_date
                    
            union all
            
            select {inactive_cust_feature_list}
            from (
                select * from input_df 
                where partition_month != (select max(partition_month) from input_df)
                    and charge_type != 'Pre-paid'
            ) df1
            left anti join input_df df2
            on df1.partition_month = df2.last_month
                    and df1.subscription_identifier = df2.subscription_identifier
        )
        select {normal_feature_list} from input_df
        union all
        select {normal_feature_list} from non_active_customer
    """.format(inactive_cust_feature_list=','.join(inactive_cust_feature_list),
               normal_feature_list=','.join(normal_feature_list)))
    return df


def merge_union_and_basic_features(union_features, basic_features):
    union_features.createOrReplaceTempView("union_features")
    basic_features.createOrReplaceTempView("basic_features")

    spark = get_spark_session()

    df = spark.sql("""
        select * 
        from union_features uf
        inner join basic_features bf
        on uf.mobile_no = bf.access_method_num
           and uf.activation_date = bf.register_date
        where bf.charge_type = 'Pre-paid'
           and uf.activation_date < last_day(to_date(cast(bf.partition_month as STRING), 'yyyyMM'))
        union
        select * 
        from union_features uf
        inner join basic_features bf
        on uf.subscription_identifier = bf.crm_sub_id
        where bf.charge_type != 'Pre-paid'
           and uf.activation_date < last_day(to_date(cast(bf.partition_month as STRING), 'yyyyMM'))
    """)

    # just pick 1 since it's join key
    df = df.drop("activation_date", "crm_sub_id", "mobile_no")

    return df


def union_monthly_cust_profile(
        cust_prof_daily_df: DataFrame
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cust_prof_daily_df]):
        return get_spark_empty_df()

    cust_prof_daily_df = data_non_availability_and_missing_check(df=cust_prof_daily_df, grouping="monthly",
                                                                 missing_data_check_flg='Y',
                                                                 par_col="event_partition_date",
                                                                 target_table_name="l3_customer_profile_union_monthly_feature")

    if check_empty_dfs([cust_prof_daily_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    cust_prof_daily_df = cust_prof_daily_df.drop("start_of_week")
    cust_prof_daily_df.createOrReplaceTempView("cust_prof_daily_df")

    sql_stmt = """
        with ranked_cust_profile as (
            select 
                *,
                row_number() over (partition by subscription_identifier, start_of_month
                                    order by event_partition_date desc) as _rnk
            from cust_prof_daily_df
        )
        select *
        from ranked_cust_profile
        where _rnk = 1
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    dates_list = cust_prof_daily_df.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    spark = get_spark_session()
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = cust_prof_daily_df.filter(f.col("start_of_month").isin(*[curr_item]))
        small_df.createOrReplaceTempView("cust_prof_daily_df")
        small_df = spark.sql(sql_stmt).drop("_rnk", "event_partition_date")
        CNTX.catalog.save("l3_customer_profile_union_monthly_feature", small_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = cust_prof_daily_df.filter(f.col("start_of_month").isin(*[first_item]))
    return_df.createOrReplaceTempView("cust_prof_daily_df")
    return_df = spark.sql(sql_stmt).drop("_rnk", "event_partition_date")

    return return_df


def add_last_month_unioned_inactive_user(
        monthly_cust_profile_df: DataFrame
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([monthly_cust_profile_df]):
        return get_spark_empty_df()

    monthly_cust_profile_df = data_non_availability_and_missing_check(df=monthly_cust_profile_df, grouping="monthly",
                                                                      par_col="start_of_month",
                                                                      target_table_name="l3_customer_profile_union_monthly_feature_include_1mo_non_active")

    if check_empty_dfs([monthly_cust_profile_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    spark = get_spark_session()

    monthly_cust_profile_df = (monthly_cust_profile_df
                               .withColumn("cust_active_this_month", f.lit("Y"))
                               .withColumn("last_month", f.add_months(f.col("start_of_month"), -1)))

    monthly_cust_profile_df.createOrReplaceTempView("input_df")

    non_active_customer_df = (spark.sql("""
                                    select *
                                    from (
                                        select * from input_df 
                                        where start_of_month != (select max(start_of_month) from input_df)
                                    ) df1
                                    left anti join input_df df2
                                    on df1.start_of_month = df2.last_month
                                            and df1.subscription_identifier = df2.subscription_identifier
                                    
                                """)
                              .withColumn("cust_active_this_month", f.lit("N"))
                              # To forward inactive customer to next month
                              .withColumn("start_of_month", f.add_months(f.col("start_of_month"), 1)))

    df = (non_active_customer_df
          .unionByName(monthly_cust_profile_df)
          .drop("last_month"))

    return df


def df_smp_for_l3_customer_profile_include_1mo_non_active(journey: DataFrame, smp_input: DataFrame):
    logging.info("Ghahiyaram")

    if check_empty_dfs([journey, smp_input]):
        return get_spark_empty_df()

    smp_pre = smp_input.dropDuplicates((["month_id", "mobile_no", "register_date", "network_type"])).where(
        "network_type='1-2-Call'")
    smp_post = smp_input.dropDuplicates((["month_id", "subscription_identifier", "network_type"])).where(
        "network_type='3G'")
    smp_pre.createOrReplaceTempView('smp_pre')
    smp_post.createOrReplaceTempView('smp_post')
    journey.createOrReplaceTempView('journey')

    spark = get_spark_session()
    # amendment_reason_code_previous
    df1 = spark.sql("""
        select a.*,
        (case when a.charge_type = 'Pre-paid' then c.amendment_reason_code
        else b.amendment_reason_code end) as amendment_reason_code_previous

        from journey a
        left join smp_post b
        on a.partition_month = substr(cast(add_months(b.month_id,1) as date),1,4)||substr(cast(add_months(b.month_id,1)as date),6,2)
        and a.crm_sub_id = b.subscription_identifier
        left join smp_pre c
        on a.partition_month = substr(cast(add_months(c.month_id,1) as date),1,4)||substr(cast(add_months(c.month_id,1)as date),6,2)
        and a.access_method_num = c.mobile_no
        and a.register_date = c.register_date
    """)
    df1.createOrReplaceTempView("journey1")
    # mobile_segment_previous
    df2 = spark.sql(""" 
       select a.*
        ,(case when a.charge_type = 'Pre-paid' then c.mobile_segment_p1
        else b.mobile_segment_p1 end) as mobile_segment_previous
        
        ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = 'Classic' and c.mobile_segment in ('Emerald ', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
        else (case when b.mobile_segment_p1 = 'Classic' and b.mobile_segment in ('Emerald ', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as classic_upgrade_yn
        
        ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 like 'Prospect%' and c.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
        else (case when b.mobile_segment_p1 like 'Prospect%' and b.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as prospect_upgrade_yn
        
        ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = c.mobile_segment and c.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
        else (case when b.mobile_segment_p1 = b.mobile_segment and b.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as serenade_sustain_yn

        ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = 'Classic' and c.mobile_segment in ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y'
                                                         when c.mobile_segment_p1 = 'Emerald' and c.mobile_segment in  ('Gold', 'Platinum', 'Platinum Plus') then 'Y'
                                                         when c.mobile_segment_p1 = 'Gold' and c.mobile_segment in ('Platinum', 'Platinum Plus') then 'Y'
                                                     else 'N' end)
        else(case when b.mobile_segment_p1 = 'Classic' and b.mobile_segment in ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y'
                   when b.mobile_segment_p1 = 'Emerald' and b.mobile_segment in  ('Gold', 'Platinum', 'Platinum Plus') then 'Y'
                   when b.mobile_segment_p1 = 'Gold' and b.mobile_segment in ('Platinum', 'Platinum Plus') then 'Y'
                   else 'N' end) end) as upgrade_to_serenade_yn

        ,(case when b.mobile_segment_p1 = 'Emerald' and b.mobile_segment ='Classic'  then 'Y'
              when b.mobile_segment_p1 = 'Gold' and b.mobile_segment in  ('Emerald', 'Classic') then 'Y'
              when b.mobile_segment_p1  in ('Platinum', 'Platinum Plus') and b.mobile_segment in ('Emerald', 'Classic', 'Gold') then 'Y'
        else 'N' end) as downgrade_serenade_yn

       from journey1 a
       left join smp_post b
       on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
       and a.crm_sub_id = b.subscription_identifier
       left join smp_pre c
       on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
       and a.access_method_num = c.mobile_no
       and a.register_date = c.register_date
    """)
    logging.info("df1 data type : {}".format(type(df1)))
    logging.info("df1 partition numbers : {}".format(df1.rdd.getNumPartitions()))
    logging.info("df2 data type : {}".format(type(df2)))
    logging.info("df2 partition numbers : {}".format(df2.rdd.getNumPartitions()))
    logging.info(("df2 repartition to : 1800"))
    df3=df2.repartition(1800)
    logging.info("df2 partition numbers : {}".format(df3.rdd.getNumPartitions()))
    return df3
    ##################  Old query
    # df1.createOrReplaceTempView("journey1")
    # # mobile_segment_previous
    # df2 = spark.sql("""
    #     select a.*,
    #     (case when a.charge_type = 'Pre-paid' then c.mobile_segment_p1
    #     else b.mobile_segment_p1 end) as mobile_segment_previous
    #
    #     from journey1 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # df2.createOrReplaceTempView("journey2")
    # # classic_upgrade_yn
    # df3 = spark.sql("""
    #     select a.*,
    #     (case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = 'Classic' and c.mobile_segment in ('Emerald ', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
    #     else (case when b.mobile_segment_p1 = 'Classic' and b.mobile_segment in ('Emerald ', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as classic_upgrade_yn
    #
    #     from journey2 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # df3.createOrReplaceTempView("journey3")
    # # prospect_upgrade_yn
    # df4 = spark.sql("""
    #     select a.*,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 like 'Prospect%' and c.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
    #     else (case when b.mobile_segment_p1 like 'Prospect%' and b.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as prospect_upgrade_yn
    #
    #     from journey3 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # df4.createOrReplaceTempView("journey4")
    # # serenade_sustain_yn
    # df5 = spark.sql("""
    #     select a.*
    #     ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = c.mobile_segment and c.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end)
    #     else (case when b.mobile_segment_p1 = b.mobile_segment and b.mobile_segment in  ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y' else 'N' end) end) as serenade_sustain_yn
    #
    #     from journey4 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # df5.createOrReplaceTempView("journey5")
    # # upgrade_to_serenade_yn
    # df6 = spark.sql("""
    #     select a.*
    #     ,(case when a.charge_type = 'Pre-paid' then (case when c.mobile_segment_p1 = 'Classic' and c.mobile_segment in ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y'
    #                                                      when c.mobile_segment_p1 = 'Emerald' and c.mobile_segment in  ('Gold', 'Platinum', 'Platinum Plus') then 'Y'
    #                                                      when c.mobile_segment_p1 = 'Gold' and c.mobile_segment in ('Platinum', 'Platinum Plus') then 'Y'
    #                                                  else 'N' end)
    #      else(case when b.mobile_segment_p1 = 'Classic' and b.mobile_segment in ('Emerald', 'Gold', 'Platinum', 'Platinum Plus') then 'Y'
    #                when b.mobile_segment_p1 = 'Emerald' and b.mobile_segment in  ('Gold', 'Platinum', 'Platinum Plus') then 'Y'
    #                when b.mobile_segment_p1 = 'Gold' and b.mobile_segment in ('Platinum', 'Platinum Plus') then 'Y'
    #                else 'N' end) end) as upgrade_to_serenade_yn
    #
    #     from journey5 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # df6.createOrReplaceTempView("journey6")
    #
    # # downgrade_serenade_yn
    # df7 = spark.sql("""
    #     select a.*,
    #     (case when b.mobile_segment_p1 = 'Emerald' and b.mobile_segment ='Classic'  then 'Y'
    #           when b.mobile_segment_p1 = 'Gold' and b.mobile_segment in  ('Emerald', 'Classic') then 'Y'
    #           when b.mobile_segment_p1  in ('Platinum', 'Platinum Plus') and b.mobile_segment in ('Emerald', 'Classic', 'Gold') then 'Y'
    #      else 'N' end) as downgrade_serenade_yn
    #
    #     from journey6 a
    #     left join smp_post b
    #     on a.partition_month = substr(cast(b.month_id as date),1,4)||substr(cast(b.month_id as date),6,2)
    #     and a.crm_sub_id = b.subscription_identifier
    #     left join smp_pre c
    #     on a.partition_month = substr(cast(c.month_id as date),1,4)||substr(cast(c.month_id as date),6,2)
    #     and a.access_method_num = c.mobile_no
    #     and a.register_date = c.register_date
    # """)
    # return df7


def df_profile_drm_t_serenade_master_post_for_l3_customer_profile_include_1mo_non_active(journey: DataFrame,
                                                                                         serenade_input: DataFrame,
                                                                                         lm_address_master: DataFrame):
    if check_empty_dfs([journey,
                        serenade_input,
                        lm_address_master]):
        return get_spark_empty_df()

    spark = get_spark_session()
    lm_address_master = lm_address_master.select('lm_prov_namt', 'lm_prov_name').distinct()
    lm_address_master.createOrReplaceTempView("lm_address_master")

    serenade_input.createOrReplaceTempView("profile_drm_t_serenade")
    journey.createOrReplaceTempView("df_journey")

    sql = """select * from(select access_method_num,register_date,network_type,channel,srn_group_status_cd,master_flag,ROW_NUMBER() OVER(PARTITION BY access_method_num,register_date ORDER BY srn_group_status_eff_dttm desc) as row from profile_drm_t_serenade) ab1 where row = 1"""
    serenade_pre = spark.sql(sql)
    serenade_pre = serenade_pre.drop("row")

    sql = """select * from(select crm_subscription_id,network_type,channel,srn_group_status_cd,master_flag,ROW_NUMBER() OVER(PARTITION BY crm_subscription_id ORDER BY srn_group_status_eff_dttm desc) as row from profile_drm_t_serenade) ab2 where row = 1"""
    serenade_post = spark.sql(sql)
    serenade_post = serenade_post.drop("row")

    serenade_pre.createOrReplaceTempView('master_pre')
    serenade_post.createOrReplaceTempView('master_post')

    # serenade_group_status
    sql = """
    select a.*,(case when a.charge_type = 'Pre-paid' then (case when b.srn_group_status_cd = 'Active' then 'Active' when b.srn_group_status_cd is not null and b.srn_group_status_cd <> 'Active' then 'Inactive' else null end) else (case when c.srn_group_status_cd = 'Active' then 'Active' when c.srn_group_status_cd is not null and c.srn_group_status_cd <> 'Active' then 'Inactive' else null end) end)  as serenade_group_status
    from df_journey a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # master_group_serenade_by_account
    df.createOrReplaceTempView("df_journey1")
    sql = """
    select a.*,(case when a.charge_type = 'Pre-paid' then (case when b.master_flag = 'Y' then 'Master Mobile' when b.master_flag = 'N' then 'Child Mobile' end) else (case when c.master_flag = 'Y' then 'Master Mobile' when c.master_flag = 'N' then 'Child Mobile' end)end) as master_group_serenade_by_account 
    from df_journey1 a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # serenade_account_flag_yn
    df.createOrReplaceTempView("df_journey2")
    sql = """
    select a.*,
    (case when a.charge_type = 'Pre-paid' then (case when b.srn_group_status_cd is not null then 'Y' else 'N' end) else (case when c.srn_group_status_cd is not null then 'Y' else 'N' end)end) as serenade_account_flag_yn
    from df_journey2 a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # serenade_by_account_channel
    df.createOrReplaceTempView("df_journey3")
    sql = """ select a.*,(case when a.charge_type = 'Pre-paid' then b.channel else c.channel end) as serenade_by_account_channel
    from df_journey3 a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # serenade_by_account_group
    df.createOrReplaceTempView("df_journey4")
    sql = """
    select a.*,(case when a.charge_type = 'Pre-paid' then b.network_type else c.network_type end) as serenade_by_account_group
    from df_journey4 a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # serenade_group_fbb_and_mobile_yn
    df.createOrReplaceTempView("df_journey5")
    sql = """
    select a.*,(case when a.charge_type = 'Pre-paid' then (case when b.network_type = 'Mobile+FBB' then 'Y' else 'N' end) else (case when c.network_type = 'Mobile+FBB' then 'Y' else 'N' end) end) as serenade_group_fbb_and_mobile_yn
    from df_journey5 a
    left join master_pre b on a.access_method_num = b.access_method_num and a.register_date = b.register_date
    left join master_post c on a.crm_sub_id = c.crm_subscription_id
    """
    df = spark.sql(sql)

    # first_act_province_en
    df.createOrReplaceTempView("df_journey6")
    sql = """
    select a.*,(case when a.first_act_province_th like "กรุงเทพ%" then "BANGKOK" else b.lm_prov_name end) as first_act_province_en
    from df_journey6 a left join lm_address_master b on a.first_act_province_th = b.lm_prov_namt
    """
    df = spark.sql(sql)

    return df

def df_customer_profile_drm_t_newsub_prepaid_history_for_l3_profile_include_1mo_non_active(journey: DataFrame,newsub_prepaid_input: DataFrame):
    if check_empty_dfs([journey]):
        return get_spark_empty_df()

    spark = get_spark_session()
    newsub_prepaid_input.createOrReplaceTempView("newsub_prepaid_input")
    df_newsub_prepaid = spark.sql("""select * from newsub_prepaid_input where last_day(date_id) = last_day(register_base_tab) """)
    df_newsub_prepaid.createOrReplaceTempView("df_newsub_prepaid")
    journey.createOrReplaceTempView("df_journey")
    sql = """
    select a.*
    ,b.activate_location as activate_location_code
    ,b.pi_location_code as pi_location_code
    ,b.pi_location_name as pi_location_name
    ,b.pi_location_region as pi_location_region
    ,b.pi_location_province as pi_location_province
    ,b.dealer_code_prep as pi_dealer_code    
    from df_journey a 
    left join df_newsub_prepaid b
    on a.access_method_num = b.mobile_no
    and a.register_date =b.register_base_tab
    and a.charge_type = 'Pre-paid'
    and b.order_type <> 'Convert Postpaid to Prepaid'
    """
    df = spark.sql(sql)
    return df


def df_feature_lot8_for_l3_profile_include_1mo_non_active(
        journey: DataFrame,
        ru_a_vas_package_daily: DataFrame,
        prepaid_identification: DataFrame,
        prepaid_identn_profile_hist: DataFrame,
        customer_profile_ma_daily: DataFrame,
        newsub_prepaid_history: DataFrame,
        partner_location_profile_monthly: DataFrame,
        lm_address_master: DataFrame
):
    if check_empty_dfs([journey,
                        ru_a_vas_package_daily,
                        prepaid_identification,
                        prepaid_identn_profile_hist,
                        customer_profile_ma_daily,
                        newsub_prepaid_history,
                        partner_location_profile_monthly,
                        lm_address_master]):
        return get_spark_empty_df()


    spark = get_spark_session()

    # university_flag
    journey.createOrReplaceTempView('journey')
    ru_a_vas_package_daily = ru_a_vas_package_daily.filter(
        (col('package_id') == '3801296') | (col('package_id') == '3801373'))
    ru_a_vas_package_daily.createOrReplaceTempView('ru_a_vas_package_daily')

    df1 = spark.sql("""
        select a.*,
        case when b.package_id is not null then 'Y' else null end as university_flag

        from journey a
        left join (select * from(select *,ROW_NUMBER() OVER(PARTITION BY c360_subscription_identifier order by partition_date desc) as row from ru_a_vas_package_daily) where row = 1) b 
        on a.c360_subscription_identifier = b.c360_subscription_identifier
        and a.partition_month = substr(cast(b.day_id as date),1,4)||substr(cast(b.day_id as date),6,2)
    """)

    df1.createOrReplaceTempView('journey')

    # immigrant_flag
    prepaid_identification.createOrReplaceTempView('prepaid_identification')
    prepaid_identn_profile_hist.createOrReplaceTempView('prepaid_identn_profile_hist')

    # df2 = spark.sql("""
    #     select a.*,
    #     (case when charge_type = 'Post-paid' then null else (case when COALESCE(b.card_type,c.card_type) is not null then 'Y' else null end) end) as immigrant_flag
    #
    #     from journey a
    #     left join (select distinct card_type,mobile_no,reg_date,prepaid_identn_end_dt from prepaid_identn_profile_hist) b
    #     on a.access_method_num = b.mobile_no
    #     and a.register_date = b.reg_date
    #     and b.card_type = '6'
    #     and b.prepaid_identn_end_dt = '9999-12-31 23:59:59'
    #
    #     left join (select distinct prepaid_identn_num,access_method_num,prepaid_identn_type_cd,new_prepaid_identn_id,prepaid_identn_type_cd as card_type from prepaid_identification) c
    #     on a.card_no = c.prepaid_identn_num
    #     and a.access_method_num = c.access_method_num
    #     and c.prepaid_identn_type_cd = '6'
    #     and c.new_prepaid_identn_id is null
    # """)
    df2 = spark.sql("""
        select a.*,   
        (case when charge_type = 'Post-paid' then null else (case when COALESCE(b.card_type,c.card_type) is not null then 'Y' else null end) end) as immigrant_flag

        from journey a 
        left join 
        (
              select * from 
              (select card_type,mobile_no,reg_date,prepaid_identn_end_dt , row_number() over (partition by mobile_no, reg_date order by prepaid_identn_end_dt) rn from prepaid_identn_profile_hist)  a
              where rn = 1 
        ) b 
        on a.access_method_num = b.mobile_no 
        and a.register_date = b.reg_date
        
        left join (select distinct prepaid_identn_num,access_method_num,prepaid_identn_type_cd,new_prepaid_identn_id,prepaid_identn_type_cd as card_type from prepaid_identification) c 
        on a.card_no = c.prepaid_identn_num
        and a.access_method_num = c.access_method_num
        and c.prepaid_identn_type_cd = '6'
        and c.new_prepaid_identn_id is null
    """)
    df2.createOrReplaceTempView('journey')

    # multisim_flag
    customer_profile_ma_daily.createOrReplaceTempView('customer_profile_ma_daily')
    df3 = spark.sql("""
        select a.*,(case when a.charge_type = 'Pre-paid' then null else (b.multisim_user_flag) end) as multisim_flag

        from journey a
        left join (select * from customer_profile_ma_daily where multisim_user_flag is not null) b        
        on a.crm_sub_id = b.crm_subscription_id
        and a.partition_month = b.partition_month
    """)
    # df3 = spark.sql("""
    #     select a.*,(case when a.charge_type = 'Pre-paid' then null else (b.multisim_user_flag) end) as multisim_flag
    #
    #     from journey a
    #     left join (select * from customer_profile_ma_daily where multisim_user_flag is not null) b
    #     on a.crm_sub_id = b.crm_subscription_id
    #     and last_day(substr(a.partition_month,1,4)||"-"||substr(a.partition_month,5,2)||"-01") = date(b.date_id)
    # """)
    df3.createOrReplaceTempView('journey')

    # pi_location_province_name_en
    # pi_location_province_name_th
    # pi_location_amphur_en
    # pi_location_amphur_th
    # pi_location_tumbol_en
    # pi_location_tumbol_th
    # pi_location_name_en
    # pi_channel_main_group
    # pi_channel_group
    # pi_channel_sub_group

    newsub_prepaid_history.createOrReplaceTempView('newsub_prepaid_history')
    partner_location_profile_monthly.createOrReplaceTempView('partner_location_profile_monthly')

    address_master_pro = lm_address_master.select('lm_prov_namt', 'lm_prov_name').distinct()
    address_master_pro.createOrReplaceTempView('address_master_pro')

    address_master_amp = lm_address_master.select('lm_prov_namt', 'lm_prov_name', 'lm_amp_namt',
                                                  'lm_amp_name').distinct()
    address_master_amp.createOrReplaceTempView('address_master_amp')

    address_master_tam = lm_address_master.select('lm_prov_namt', 'lm_prov_name', 'lm_amp_namt', 'lm_amp_name',
                                                  'lm_tam_namt', 'lm_tam_name').distinct()
    address_master_tam.createOrReplaceTempView('address_master_tam')

    # df4 = spark.sql("""
    #     select a.*,UPPER(coalesce(d.lm_prov_name,c.location_province_name_en)) as pi_location_province_name_en
    #           ,c.location_province_name_th as pi_location_province_name_th
    #
    #           ,UPPER(coalesce(e.lm_amp_name,c.location_amphur_en)) as pi_location_amphur_en
    #           ,c.location_amphur_th as pi_location_amphur_th
    #
    #           ,UPPER(coalesce(f.lm_tam_name,c.location_tumbol_en)) as pi_location_tumbol_en
    #           ,c.location_tumbol_th as pi_location_tumbol_th
    #
    #           ,c.location_name_en as pi_location_name_en
    #
    #           ,c.channel_main_group as pi_channel_main_group
    #
    #           ,c.channel_group as pi_channel_group
    #
    #           ,c.channel_sub_group as pi_channel_sub_group
    #
    #     from journey a
    #
    #     left join (select * from newsub_prepaid_history where order_type not in ('Convert Postpaid to Prepaid') and last_day(date_id) = last_day(register_base_tab)) b
    #     on a.access_method_num = b.mobile_no
    #     and a.register_date = b.register_base_tab
    #
    #     left join partner_location_profile_monthly c
    #     on b.pi_location_code = c.location_cd
    #     and c.month_id = (select max(month_id) from partner_location_profile_monthly)
    #
    #     left join address_master_pro d
    #     on c.location_province_name_th = d.lm_prov_namt
    #
    #     left join address_master_amp e
    #     on c.location_province_name_th = e.lm_prov_namt
    #     and c.location_amphur_th = e.lm_amp_namt
    #
    #     left join address_master_tam f
    #     on c.location_province_name_th = f.lm_prov_namt
    #     and c.location_amphur_th = f.lm_amp_namt
    #     and c.location_tumbol_th = f.lm_tam_namt
    # """)

    df4 = spark.sql("""
        select a.*,UPPER(coalesce(d.lm_prov_name,c.location_province_name_en)) as pi_location_province_name_en
              ,c.location_province_name_th as pi_location_province_name_th

              ,UPPER(coalesce(e.lm_amp_name,c.location_amphur_en)) as pi_location_amphur_en
              ,c.location_amphur_th as pi_location_amphur_th

              ,UPPER(coalesce(f.lm_tam_name,c.location_tumbol_en)) as pi_location_tumbol_en
              ,c.location_tumbol_th as pi_location_tumbol_th

              ,c.location_name_en as pi_location_name_en

              ,c.channel_main_group as pi_channel_main_group

              ,c.channel_group as pi_channel_group

              ,c.channel_sub_group as pi_channel_sub_group

        from journey a 

        left join newsub_prepaid_history b
        on a.access_method_num = b.mobile_no
        and a.register_date = b.register_base_tab

        left join partner_location_profile_monthly c
        on b.pi_location_code = c.location_cd
  
        left join address_master_pro d
        on c.location_province_name_th = d.lm_prov_namt

        left join address_master_amp e
        on c.location_province_name_th = e.lm_prov_namt
        and c.location_amphur_th = e.lm_amp_namt

        left join address_master_tam f
        on c.location_province_name_th = f.lm_prov_namt 
        and c.location_amphur_th = f.lm_amp_namt
        and c.location_tumbol_th = f.lm_tam_namt
    """)

    return df4


def df_ma_daily_to_multisum_for_l3_profile_include_1mo_non_active(input_df):
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_date",
                                                       target_table_name="int_l3_customer_profile_multisum_monthly")

    df = input_df.withColumn('partition_month', f.expr('substr(cast(partition_date as string),1,6)'))\
        .withColumn('date_date', f.expr("to_date(cast(partition_date as string),'yyyyMMdd')"))\
        .withColumn('eom', f.expr('last_day(date_date)'))\
        .withColumn('start_of_month', f.expr("to_date(partition_month||'01','yyyyMMdd')"))\
        .where("date_date = eom and multisim_user_flag is not null")\
        .select("start_of_month","partition_month", "crm_subscription_id", "multisim_user_flag")

    return df

def df_customer_profile_pp_identn_profile_hist_for_l3_customer_profile_include_1mo_non_active(pp_iden_data):
    if check_empty_dfs([pp_iden_data]):
        return get_spark_empty_df()
    df = pp_iden_data.where("card_type = '6' and cast(prepaid_identn_end_dt as date) >= to_date('9999-12-31','yyyy-MM-dd')")\
                    .select("card_type", "mobile_no", "reg_date", "prepaid_identn_end_dt")\
                    .dropDuplicates(["card_type", "mobile_no", "reg_date", "prepaid_identn_end_dt"])
    return df

def df_customer_profile_newsub_for_l3_customer_profile_include_1mo_non_active(input_df):
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    spark = get_spark_session()
    input_df.createOrReplaceTempView('newsub_prepaid_history_tmp')
    df = spark.sql("select * from newsub_prepaid_history_tmp where order_type not in ('Convert Postpaid to Prepaid') and last_day(date_id) = last_day(register_base_tab)")
    return df