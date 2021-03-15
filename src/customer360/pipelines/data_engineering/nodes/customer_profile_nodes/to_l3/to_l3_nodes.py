from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from kedro.context.context import load_context
from pathlib import Path
import os, logging
from pyspark.sql import DataFrame, functions as f

conf = os.getenv("CONF", None)


def df_copy_for_l3_customer_profile_include_1mo_non_active(input_df):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_customer_profile_include_1mo_non_active")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


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
