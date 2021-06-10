from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols, get_max_date_from_master_data, gen_max_sql, execute_sql

from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import StringType
import logging


def l4_billing_last_and_most_billing_payment_detail(
        inputDF, paymentMst, profileDF
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([inputDF, paymentMst, profileDF]):
        return get_spark_empty_df()

    profileDF = profileDF.where("charge_type = 'Post-paid' and cust_active_this_month = 'Y'")\
        .withColumn("start_of_month", profileDF.partition_month)

    inputDF = inputDF.withColumn("start_of_month",
                                     f.to_date(f.date_trunc('month', f.to_date((f.col('payment_date'))))))


    profileDF = data_non_availability_and_missing_check(df=profileDF, grouping="monthly",
                                                        par_col="start_of_month",
                                                        target_table_name="l4_billing_last_and_most_billing_payment_detail")

    inputDF = data_non_availability_and_missing_check(df=inputDF, grouping="monthly",
                                                        par_col="partition_date",
                                                        target_table_name="l4_billing_last_and_most_billing_payment_detail",
                                                        missing_data_check_flg='Y')

    min_value = union_dataframes_with_missing_cols(
        [
            inputDF.select(f.max(f.col("start_of_month")).alias("max_date")),
            profileDF.select(f.max(f.col("start_of_month")).alias("max_date"))
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    inputDF = inputDF.filter(f.col("start_of_month") <= min_value)
    profileDF = profileDF.filter(f.col("start_of_month") <= min_value)


    paymentMst = get_max_date_from_master_data(paymentMst, 'partition_date')

    if check_empty_dfs([inputDF, profileDF]):
        return get_spark_empty_df()

    # profileDF = profileDF.withColumn("no_of_sub", f.expr(
    #       "count(subscription_identifier) over (partition by start_of_month ,billing_account_no)"))

    inputDF = inputDF.withColumn("rn", f.expr(
        "row_number() over (partition by payment_identifier order by partition_date desc)")).where("rn = 1").drop("rn")

    profileDF = profileDF.select("start_of_month", "subscription_identifier",  "billing_account_no")\
        .withColumnRenamed("billing_account_no", "ba_no")\
        .withColumn("end_of_month", f.last_day(f.col("start_of_month")))
    profileDF.createOrReplaceTempView('profile')

    spark = get_spark_session()
    inputDF.createOrReplaceTempView("payment")
    paymentMst.createOrReplaceTempView('mst')

    pymtSelectedDF = inputDF.join(paymentMst, (inputDF.payment_channel == paymentMst.payment_channel_code))\
        .select("payment_identifier","no_of_days", "account_identifier", "ba_no", "payment_date", "payment_method",\
        "payment_channel_group", "payment_channel_type")\
        .withColumn("payment_status", f.expr("case when no_of_days = 0 then 'on due' when no_of_days < 0 then 'before due' else 'over due' end"))

    # pymtSelectedDF = spark.sql("""
    #       select *, case when no_of_days = 0 then 'on due' when no_of_days < 0 then 'before due'
    #                        else 'over due' end as payment_status
    #       from
    #       (
    #           select
    #               p.payment_identifier,
    #               p.start_of_month,
    #               p.account_identifier,
    #               p.ba_no,
    #               p.payment_date,
    #               p.payment_method,
    #               m.payment_channel_group,
    #               m.payment_channel_type,
    #               max(p.no_of_days) no_of_days
    #           from payment p
    #           left join mst m
    #           on p.payment_channel = m.payment_channel_code
    #           group by 1,2,3,4,5,6,7,8
    #       ) a
    #   """)
    pymtDF = profileDF.join(pymtSelectedDF, (['ba_no']), 'left')

    pymt6m = pymtDF.where("payment_date between date_sub(end_of_month, - 180) and end_of_month or end_of_month is null")
    pymt3m = pymtDF.where("payment_date between date_sub(end_of_month, - 90) and end_of_month  or end_of_month is null")

    ##### last 6 month
    lastDF = pymt6m.withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier, subscription_identifier order by payment_date desc)")).where("rn = 1").drop("rn")


    lastDF = lastDF.withColumnRenamed("payment_date", "last_6mth_payment_date") \
        .withColumnRenamed("payment_method", "last_6mth_payment_method") \
        .withColumnRenamed("payment_channel_group", "last_6mth_payment_channel_group") \
        .withColumnRenamed("payment_channel_type", "last_6mth_payment_channel_type") \
        .withColumnRenamed("payment_status", "last_6mth_payment_status")

    ###### most 3 month
    most3mChennelDF = pymt3m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_channel_group",
         "payment_channel_type"]).agg(f.max("payment_date").alias("payment_date"),
                                      f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most3mMethodDF = pymt3m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_method", ]).agg(
        f.max("payment_date").alias("payment_date"), f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most3mStatusDF = pymt3m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_status", ]).agg(
        f.max("payment_date").alias("payment_date"), f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most3mDF = union_dataframes_with_missing_cols([most3mChennelDF, most3mMethodDF, most3mStatusDF])

    most3mDF = most3mDF.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier"]).agg(
        f.max("payment_channel_group").alias("most_3mth_payment_channel_group"),
        f.max("payment_channel_type").alias("most_3mth_payment_channel_type"),
        f.max("payment_method").alias("most_3mth_payment_method"),
        f.max("payment_status").alias("most_3mth_payment_status"))

    ###### most 6 month
    most6mChennelDF = pymt6m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_channel_group",
         "payment_channel_type"]).agg(f.max("payment_date").alias("payment_date"),
                                      f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most6mMethodDF = pymt6m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_method", ]).agg(
        f.max("payment_date").alias("payment_date"), f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most6mStatusDF = pymt6m.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier", "payment_status", ]).agg(
        f.max("payment_date").alias("payment_date"), f.count("*").alias("no_of_pay")).withColumn("rn", f.expr(
        "row_number() over (partition by start_of_month, account_identifier,subscription_identifier order by no_of_pay desc, payment_date desc)")).where(
        "rn = 1").drop("rn", "no_of_pay", "payment_date")

    most6mDF = union_dataframes_with_missing_cols([most6mChennelDF, most6mMethodDF, most6mStatusDF])

    most6mDF = most6mDF.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier"]).agg(
        f.max("payment_channel_group").alias("most_6mth_payment_channel_group"),
        f.max("payment_channel_type").alias("most_6mth_payment_channel_type"),
        f.max("payment_method").alias("most_6mth_payment_method"),
        f.max("payment_status").alias("most_6mth_payment_status"))

    resultDF = union_dataframes_with_missing_cols([lastDF, most6mDF, most3mDF])

    resultDF = resultDF.groupBy(
        ["start_of_month", "account_identifier", "subscription_identifier"]).agg(
        f.max("last_6mth_payment_date").alias("last_6mth_payment_date"),
        f.max("last_6mth_payment_channel_group").alias("last_6mth_payment_channel_group"),
        f.max("last_6mth_payment_channel_type").alias("last_6mth_payment_channel_type"),
        f.max("last_6mth_payment_method").alias("last_6mth_payment_method"),
        f.max("last_6mth_payment_status").alias("last_6mth_payment_status"),
        f.max("most_3mth_payment_channel_group").alias("most_3mth_payment_channel_group"),
        f.max("most_3mth_payment_channel_type").alias("most_3mth_payment_channel_type"),
        f.max("most_3mth_payment_method").alias("most_3mth_payment_method"),
        f.max("most_3mth_payment_status").alias("most_3mth_payment_status"),
        f.max("most_6mth_payment_channel_group").alias("most_6mth_payment_channel_group"),
        f.max("most_6mth_payment_channel_type").alias("most_6mth_payment_channel_type"),
        f.max("most_6mth_payment_method").alias("most_6mth_payment_method"),
        f.max("most_6mth_payment_status").alias("most_6mth_payment_status"))

    return resultDF


def l4_billing_rolling_window_top_up_channels(input_first: DataFrame,
                                              input_second: DataFrame,
                                              input_third: DataFrame,
                                              input_fourth: DataFrame,
                                              input_fifth: DataFrame) -> DataFrame:
    """
    :param input_first:
    :param input_second:
    :param input_third:
    :param input_fourth:
    :param input_fifth:
    :return:
    """
    # start_period = '2020-01-27'
    # end_period = '2020-02-17'
    # input_sum1 = input_sum1.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_sum2 = input_sum2.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_sum3 = input_sum3.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_sum4 = input_sum4.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    #
    # input_avg1 = input_avg1.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_avg2 = input_avg2.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_avg3 = input_avg3.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # input_avg4 = input_avg4.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    # logging.info(start_period+" "+end_period)


    if check_empty_dfs([input_first, input_second, input_third, input_fourth, input_fifth ]):
        return get_spark_empty_df()

    group_cols = ["subscription_identifier", "start_of_week"]
    input_first = input_first.cache()
    input_second = input_second.cache()
    input_third = input_third.cache()
    input_fourth = input_fourth.cache()
    input_fifth = input_fifth.cache()

    union_df = union_dataframes_with_missing_cols([input_first, input_second, input_third, input_fourth, input_fifth])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df

    # result_df = result_df.groupBy(["subscription_identifier","start_of_week"]).agg(
    #          f.max("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_week").alias("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_two_week").alias("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_four_week").alias("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_twelve_week").alias("sum_payments_top_ups_by_bank_atm_cdm_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_week").alias("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_two_week").alias("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_four_week").alias("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_cash_card_weekly_last_week").alias("sum_payments_top_ups_by_cash_card_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_cash_card_weekly_last_two_week").alias("sum_payments_top_ups_by_cash_card_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_cash_card_weekly_last_four_week").alias("sum_payments_top_ups_by_cash_card_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_cash_card_weekly_last_twelve_week").alias("sum_payments_top_ups_by_cash_card_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_cash_card_weekly_last_week").alias("sum_payments_top_up_vol_by_cash_card_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_cash_card_weekly_last_two_week").alias("sum_payments_top_up_vol_by_cash_card_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_cash_card_weekly_last_four_week").alias("sum_payments_top_up_vol_by_cash_card_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_cash_card_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_cash_card_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_digital_online_self_service_weekly_last_week").alias("sum_payments_top_ups_by_digital_online_self_service_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_digital_online_self_service_weekly_last_two_week").alias("sum_payments_top_ups_by_digital_online_self_service_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_digital_online_self_service_weekly_last_four_week").alias("sum_payments_top_ups_by_digital_online_self_service_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_digital_online_self_service_weekly_last_twelve_week").alias("sum_payments_top_ups_by_digital_online_self_service_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_week").alias("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_two_week").alias("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_four_week").alias("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_epin_slip_weekly_last_week").alias("sum_payments_top_ups_by_epin_slip_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_epin_slip_weekly_last_two_week").alias("sum_payments_top_ups_by_epin_slip_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_epin_slip_weekly_last_four_week").alias("sum_payments_top_ups_by_epin_slip_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_epin_slip_weekly_last_twelve_week").alias("sum_payments_top_ups_by_epin_slip_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_epin_slip_weekly_last_week").alias("sum_payments_top_up_vol_by_epin_slip_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_epin_slip_weekly_last_two_week").alias("sum_payments_top_up_vol_by_epin_slip_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_epin_slip_weekly_last_four_week").alias("sum_payments_top_up_vol_by_epin_slip_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_epin_slip_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_epin_slip_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_epos_weekly_last_week").alias("sum_payments_top_ups_by_epos_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_epos_weekly_last_two_week").alias("sum_payments_top_ups_by_epos_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_epos_weekly_last_four_week").alias("sum_payments_top_ups_by_epos_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_epos_weekly_last_twelve_week").alias("sum_payments_top_ups_by_epos_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_epos_weekly_last_week").alias("sum_payments_top_up_vol_by_epos_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_epos_weekly_last_two_week").alias("sum_payments_top_up_vol_by_epos_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_epos_weekly_last_four_week").alias("sum_payments_top_up_vol_by_epos_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_epos_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_epos_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_rom_weekly_last_week").alias("sum_payments_top_ups_by_rom_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_rom_weekly_last_two_week").alias("sum_payments_top_ups_by_rom_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_rom_weekly_last_four_week").alias("sum_payments_top_ups_by_rom_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_rom_weekly_last_twelve_week").alias("sum_payments_top_ups_by_rom_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_rom_weekly_last_week").alias("sum_payments_top_up_vol_by_rom_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_rom_weekly_last_two_week").alias("sum_payments_top_up_vol_by_rom_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_rom_weekly_last_four_week").alias("sum_payments_top_up_vol_by_rom_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_rom_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_rom_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_mpay_non_vending_weekly_last_week").alias("sum_payments_top_ups_by_mpay_non_vending_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_mpay_non_vending_weekly_last_two_week").alias("sum_payments_top_ups_by_mpay_non_vending_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_mpay_non_vending_weekly_last_four_week").alias("sum_payments_top_ups_by_mpay_non_vending_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_mpay_non_vending_weekly_last_twelve_week").alias("sum_payments_top_ups_by_mpay_non_vending_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_week").alias("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_two_week").alias("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_four_week").alias("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_mpay_non_vending_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_ups_by_mpay_vending_weekly_last_week").alias("sum_payments_top_ups_by_mpay_vending_weekly_last_week"),
    #          f.max("sum_payments_top_ups_by_mpay_vending_weekly_last_two_week").alias("sum_payments_top_ups_by_mpay_vending_weekly_last_two_week"),
    #          f.max("sum_payments_top_ups_by_mpay_vending_weekly_last_four_week").alias("sum_payments_top_ups_by_mpay_vending_weekly_last_four_week"),
    #          f.max("sum_payments_top_ups_by_mpay_vending_weekly_last_twelve_week").alias("sum_payments_top_ups_by_mpay_vending_weekly_last_twelve_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_vending_weekly_last_week").alias("sum_payments_top_up_vol_by_mpay_vending_weekly_last_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_vending_weekly_last_two_week").alias("sum_payments_top_up_vol_by_mpay_vending_weekly_last_two_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_vending_weekly_last_four_week").alias("sum_payments_top_up_vol_by_mpay_vending_weekly_last_four_week"),
    #          f.max("sum_payments_top_up_vol_by_mpay_vending_weekly_last_twelve_week").alias("sum_payments_top_up_vol_by_mpay_vending_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_two_week").alias("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_two_week"),
    #          f.max("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_four_week").alias("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_four_week"),
    #          f.max("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_twelve_week").alias("avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_week").alias("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_two_week").alias("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_two_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_four_week").alias("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_four_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_twelve_week").alias("avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_ups_avg_by_epin_slip_weekly_last_week").alias("avg_payments_top_ups_avg_by_epin_slip_weekly_last_week"),
    #          f.max("avg_payments_top_ups_avg_by_epin_slip_weekly_last_two_week").alias("avg_payments_top_ups_avg_by_epin_slip_weekly_last_two_week"),
    #          f.max("avg_payments_top_ups_avg_by_epin_slip_weekly_last_four_week").alias("avg_payments_top_ups_avg_by_epin_slip_weekly_last_four_week"),
    #          f.max("avg_payments_top_ups_avg_by_epin_slip_weekly_last_twelve_week").alias("avg_payments_top_ups_avg_by_epin_slip_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_week").alias("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_two_week").alias("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_two_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_four_week").alias("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_four_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_twelve_week").alias("avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_ups_avg_by_epos_weekly_last_week").alias("avg_payments_top_ups_avg_by_epos_weekly_last_week"),
    #          f.max("avg_payments_top_ups_avg_by_epos_weekly_last_two_week").alias("avg_payments_top_ups_avg_by_epos_weekly_last_two_week"),
    #          f.max("avg_payments_top_ups_avg_by_epos_weekly_last_four_week").alias("avg_payments_top_ups_avg_by_epos_weekly_last_four_week"),
    #          f.max("avg_payments_top_ups_avg_by_epos_weekly_last_twelve_week").alias("avg_payments_top_ups_avg_by_epos_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epos_weekly_last_week").alias("avg_payments_top_up_vol_avg_by_epos_weekly_last_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epos_weekly_last_two_week").alias("avg_payments_top_up_vol_avg_by_epos_weekly_last_two_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epos_weekly_last_four_week").alias("avg_payments_top_up_vol_avg_by_epos_weekly_last_four_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_epos_weekly_last_twelve_week").alias("avg_payments_top_up_vol_avg_by_epos_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_ups_avg_by_rom_weekly_last_week").alias("avg_payments_top_ups_avg_by_rom_weekly_last_week"),
    #          f.max("avg_payments_top_ups_avg_by_rom_weekly_last_two_week").alias("avg_payments_top_ups_avg_by_rom_weekly_last_two_week"),
    #          f.max("avg_payments_top_ups_avg_by_rom_weekly_last_four_week").alias("avg_payments_top_ups_avg_by_rom_weekly_last_four_week"),
    #          f.max("avg_payments_top_ups_avg_by_rom_weekly_last_twelve_week").alias("avg_payments_top_ups_avg_by_rom_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_rom_weekly_last_week").alias("avg_payments_top_up_vol_avg_by_rom_weekly_last_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_rom_weekly_last_two_week").alias("avg_payments_top_up_vol_avg_by_rom_weekly_last_two_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_rom_weekly_last_four_week").alias("avg_payments_top_up_vol_avg_by_rom_weekly_last_four_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_rom_weekly_last_twelve_week").alias("avg_payments_top_up_vol_avg_by_rom_weekly_last_twelve_week"),
    #          f.max("avg_payments_top_up_vol_avg_by_rom_weekly_last_twelve_week").alias("avg_payments_top_up_vol_avg_by_rom_weekly_last_twelve_week"))
    # return result_df