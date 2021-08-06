import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
from customer360.utilities.spark_util import get_spark_session
import logging

import time
import datetime


def format_time(elapsed):
    '''
    Takes a time in seconds and returns a string hh:mm:ss
    '''
    # Round to the nearest second.
    elapsed_rounded = int(round((elapsed)))

    # Format as hh:mm:ss
    return str(datetime.timedelta(seconds=elapsed_rounded))

def drop_partition(start_date, end_date, table, partition_key):
    """
    This Function is created by Titsanu, modified by Thanasit
    Args:
        start_date: datetime format of start date to delete partition
        end_date: datetime format of end date to delete partition
        table: hive table name
        partition_key: partition key column

    Returns: Does not return value

    """
    import datetime

    spark = get_spark_session()
    # swap date
    if start_date < end_date:
        stmt = (
                "DELETE FROM "
                + table
                + " WHERE date('"
                + partition_key
                + "') >= date('"
                + datetime.datetime.strftime(
            start_date, "%Y-%m-%d")
                + "') AND date('"
                + partition_key
                + "') <= date('"
                + datetime.datetime.strftime(
            end_date, "%Y-%m-%d")
                + "')"
            )
        spark.sql(stmt)
        # temp = start_date
        # start_date = end_date
        # end_date = temp
    # find the number of days between two days
    # time_diff = (start_date - end_date).days
    #
    # # time_diff + 1 to make it inclusive (normally the end_date will be excluded)
    # for i in range(0, int(time_diff) + 1):
    #     # print(start_date-timedelta(days=i))
    #     # startdropping from the start_date ---> end_date
    #     drop_date = datetime.datetime.strftime(
    #         start_date - datetime.timedelta(days=i), "%Y-%m-%d"
    #     )
    #     print("Dropping partition = " + drop_date)
    #     stmt = (
    #         "DELETE FROM "
    #         + table
    #         + " WHERE date('"
    #         + partition_key
    #         + "') = date('"
    #         + drop_date
    #         + "')"
    #     )
    #     print(stmt)
    #     try:
    #         spark.sql(stmt)
    #         print("SUCCESS")
    #     except Exception as e:
    #         print("Error at partition = " + drop_date)
    # #         print(e)
    # if time_diff < 0:
    #     print("start date must be higher than the end date")
    spark.sql("REFRESH TABLE "+table)


def save_folded_package(start_date, end_date, table, partition_key):
    """
    This function save ontop normalized revenue transaction into temporary table
    Args:
        start_date: datetime format of start date
        end_date: datetime format of end date
        table: hive table name
        partition_key: partition key column

    Returns: Does not return value

    """
    spark = get_spark_session()
    spark.sql("DROP TABLE IF EXISTS " + table + "_tmp_fold")
    # Save Normalized On-top package record that has been activated in the previous period
    # But only took effect on the update period
    sdf = spark.sql(
        "SELECT * FROM "
        + table
        + " WHERE ontop_start_date < date('"
        + start_date.strftime("%Y-%m-%d")
        + "') AND partition_date >= date('"
        + start_date.strftime("%Y-%m-%d")
        + "') AND partition_date <= date('"
        + end_date.strftime("%Y-%m-%d")
        + "')"
    )
    sdf.write.format("delta").mode("append").partitionBy(partition_key).saveAsTable(
        table + "_tmp_fold"
    )


def create_daily_ontop_pack(
    l0_product_pru_m_ontop_master_for_weekly_full_load: pyspark.sql.DataFrame,
    l1_customer_profile_union_daily_feature_full_load: pyspark.sql.DataFrame,
    ontop_pack: pyspark.sql.DataFrame,
    usage_feature: pyspark.sql.DataFrame,
    hive_table,
    start_date=None,
    end_date=None,
    drop_replace_partition=False,
):
    """
    This function took ontop transaction, ontop package master, and daily customer profile, and daily usage as inputs,
    performing revenue normalization for ontop package that has validity more than one day, which normally be ignored
    in most business logic, which could cause the wrong interpret of how customer spend on daily/weekly/monthly basis
    as the revenue will be seen as one spike for larger validity on-top packages
    Args:
        l0_product_pru_m_ontop_master_for_weekly_full_load: On-top package Master Weekly
        l1_customer_profile_union_daily_feature_full_load: C360 Customer Profile Daily
        ontop_pack: Old Cloud data-source Ontop Purchase Transaction Data/Voice Aggregated Daily
        usage_feature: Old Cloud data-source Usage Features Daily
        hive_table: Hive table name to save table
        start_date: datetime format of start date to update data
        end_date: datetime format of end date to update data
        drop_replace_partition: Boolean, Perform drop partition before updating

    Returns: Daily Revenue Normalised On-top Package with Usage Features

    """
    import datetime
    t0 = time.time()
    spark = get_spark_session()

    if start_date is None:
        start_date = datetime.datetime.now() + datetime.timedelta(days=-40)
        end_date = datetime.datetime.now() + datetime.timedelta(days=-10)

    # Drop partition that already existed to avoid data duplication
    if drop_replace_partition:
        table = "prod_dataupsell." + hive_table
        partition_key = "partition_date"
        save_folded_package(start_date, end_date, table, partition_key)
        drop_partition(start_date, end_date, table, partition_key)

    # Select period of data to work on update
    selected_l0_ontop = ontop_pack.where(
        "ddate >= date('"
        + start_date.strftime("%Y-%m-%d")
        + "') AND ddate <= date('"
        + end_date.strftime("%Y-%m-%d")
        + "')"
    )

    # Select On-top transaction
    ontop_pack_daily = selected_l0_ontop.selectExpr(
        "analytic_id",
        "crm_subscription_id as old_subscription_identifier",
        "register_date",
        "total_net_tariff",
        "promotion_code",
        "number_of_transaction",
        "date_id as partition_date",
    )

    # Select daily Customer Profile
    customer_profile_daily = l1_customer_profile_union_daily_feature_full_load.selectExpr(
        "old_subscription_identifier",
        "access_method_num",
        "date(register_date) as register_date",
        "date(event_partition_date) as partition_date",
        "date(start_of_month) as start_of_month",
        "start_of_week",
    )

    # Select period of data to work on update
    customer_profile_daily = customer_profile_daily.where(
        "partition_date >= date('" + start_date.strftime("%Y-%m-%d") + "')"
    )

    # Only Select Pre-paid Charge type, Convert partition_date to date format
    master_ontop_weekly = (
        l0_product_pru_m_ontop_master_for_weekly_full_load.where(
            "charge_type = 'Prepaid'"
        )
        .withColumn(
            "partition_date_str",
            l0_product_pru_m_ontop_master_for_weekly_full_load["partition_date"].cast(
                StringType()
            ),
        )
        .drop("partition_date")
        .select(
            "price_inc_vat",
            "package_type",
            "promotion_code",
            "package_group",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "package_name_report",
            "data_quota",
            "duration",
            F.to_timestamp("partition_date_str", "yyyyMMdd").alias(
                "partition_date_timestamp"
            ),
        )
        .selectExpr("*", "date(partition_date_timestamp) as partition_date")
        .drop("partition_date_timestamp")
    )

    # Cleansing Master data
    ##
    # Data Quota are transformed from string data to float value of MB, All unlimited are impute with extreme value
    # while null are impute with -1
    ##
    # Data Speed are inferred by it speed, the unit use is Kbps
    ##
    # This weekly data is always updated on Wednesday, However C360 Weekly Data Define start of week by Monday
    # So, we -2 partition_date to get start_of_week for data mapping
    master_ontop_weekly_fixed = master_ontop_weekly.selectExpr(
        "price_inc_vat",
        "package_type",
        "promotion_code",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        """CASE WHEN data_quota LIKE '%GB%' THEN split(data_quota, 'GB')[0] * 1024
                WHEN data_quota LIKE '%MB%' THEN split(data_quota, 'MB')[0]
                WHEN data_quota LIKE '%Hr%' THEN 999999999
                WHEN data_quota LIKE '%D' THEN 999999999
                WHEN data_quota LIKE '%M%' THEN 999999999
                WHEN data_quota LIKE '%Mins%' THEN 999999999
                WHEN data_quota LIKE '%UL%' THEN 999999999
                ELSE -1 END AS data_quota_mb""",
        """CASE WHEN duration LIKE '%D%' THEN split(duration, 'D')[0] 
                                        WHEN duration LIKE '%Hr%' THEN split(duration, 'Hr')[0]/24
                                     ELSE 0 END AS duration """,
        """CASE WHEN mm_data_Speed = 'Entertain' THEN 4096
            WHEN mm_data_Speed = 'Social' THEN 512
            WHEN mm_data_Speed = 'Time' THEN 51200
            WHEN mm_data_Speed = 'Others' THEN 512
            WHEN MM_Data_Speed = '64Kbps'				 THEN    64
            WHEN MM_Data_Speed = '256Kbps'			 THEN   256
      WHEN MM_Data_Speed = '384Kbps'			 THEN   384
      WHEN MM_Data_Speed = '512Kbps'			 THEN   512
      WHEN MM_Data_Speed = '1Mbps'				 THEN  1024
      WHEN MM_Data_Speed = '2Mbps'  			 THEN  2048
      WHEN MM_Data_Speed = '4Mbps'				 THEN  4096
      WHEN MM_Data_Speed = '6Mbps'		    	 THEN  6144
      WHEN MM_Data_Speed = '10Mbps'				 THEN 10240
      WHEN MM_Data_Speed = 'Full speed'			 THEN 51200
      WHEN MM_Data_Speed = 'Full Speed'		     THEN 51200
      WHEN MM_Data_Speed = 'Full Speed - Next G' THEN 51200
      WHEN MM_Data_Speed = '7.2Mbps'             THEN 7372
            ELSE 0
            END as data_speed""",
        "DATE_ADD(partition_date,-2) as start_of_week",
    )

    # Join Daily data, while ontop master is weekly
    daily_ontop_purchase = ontop_pack_daily.join(
        customer_profile_daily,
        ["old_subscription_identifier", "register_date", "partition_date"],
        "inner",
    ).join(master_ontop_weekly_fixed, ["promotion_code", "start_of_week"], "inner")

    # On-top package with 1 day or Less Validity can be join with usage feature directly
    one_day_ontop = daily_ontop_purchase.where("duration <= 1")

    # On-top Package that has more than 1 day validity revenue will be divided by it's validity
    # After being join with daily usage data, this will enable daily revenue normalization
    multiple_day_ontop = (
        daily_ontop_purchase.where("duration > 1")
        .selectExpr(
            "*",
            "total_net_tariff/duration as distributed_daily_spending",
            "partition_date AS ontop_start_date",
            "date_add(date(partition_date), (COALESCE(duration,2)-1)) AS ontop_end_date",
        )
        .drop("partition_date")
    )
    usage_feature = usage_feature.selectExpr(
        "analytic_id",
        "day_id as partition_date",
        "data_sum",
        f"voice_offnet_out_dursum + voice_onnet_out_post_dursum + voice_onnet_out_pre_dursum"
        + f" as voice_call_out_duration_sum",
    )
    one_day_ontop_usage = one_day_ontop.join(
        usage_feature, ["analytic_id", "partition_date"], "left"
    )

    # On-top package with multiple days of validity need to be treat differently, by joining usage within
    # the period that particular on-top package validity is still valid
    # this will give us normalized revenue and usage features for larger on-top pack
    # and can be use for further analysis
    cond = [
        multiple_day_ontop.analytic_id == usage_feature.analytic_id,
        multiple_day_ontop.ontop_end_date >= usage_feature.partition_date,
        multiple_day_ontop.ontop_end_date <= usage_feature.partition_date,
    ]
    multiple_day_ontop_usage = multiple_day_ontop.join(usage_feature, cond, "left")
    one_day_ontop_columns = [
        "analytic_id",
        "partition_date",
        "promotion_code",
        "start_of_week",
        "old_subscription_identifier",
        "register_date",
        "total_net_tariff",
        "number_of_transaction",
        "access_method_num",
        "start_of_month",
        "price_inc_vat",
        "package_type",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        "data_quota_mb",
        "duration",
        "data_speed",
        "data_sum",
        "voice_call_out_duration_sum",
        "partition_date as ontop_start_date",
        "partition_date as ontop_end_date",
    ]
    multiple_day_ontop_columns = [
        "prod_delta.dm42_promotion_prepaid.analytic_id",
        "partition_date",
        "promotion_code",
        "start_of_week",
        "old_subscription_identifier",
        "register_date",
        "distributed_daily_spending as total_net_tariff",
        "number_of_transaction",
        "access_method_num",
        "start_of_month",
        "price_inc_vat",
        "package_type",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        "data_quota_mb",
        "duration",
        "data_speed",
        "data_sum",
        "voice_call_out_duration_sum",
        "ontop_start_date",
        "ontop_end_date",
    ]
    output = (
        one_day_ontop_usage.selectExpr(one_day_ontop_columns)
        .union(multiple_day_ontop_usage.selectExpr(multiple_day_ontop_columns))
        .withColumn("partition_date_str", F.date_format("partition_date", "yyyyMMdd"))
    )
    if drop_replace_partition:
        tmp_fold = spark.sql(
            "SELECT * FROM prod_dataupsell." + hive_table + "_tmp_fold"
        )
        fold_column = [
            "analytic_id",
            "partition_date",
            "promotion_code",
            "start_of_week",
            "old_subscription_identifier",
            "register_date",
            "total_net_tariff",
            "number_of_transaction",
            "access_method_num",
            "start_of_month",
            "price_inc_vat",
            "package_type",
            "package_group",
            "mm_types",
            "mm_data_type",
            "mm_data_speed",
            "package_name_report",
            "data_quota_mb",
            "duration",
            "data_speed",
            "data_sum",
            "voice_call_out_duration_sum",
            "ontop_start_date",
            "ontop_end_date",
        ]

        output = output.union(
            tmp_fold.selectExpr(fold_column).withColumn(
                "partition_date_str", F.date_format("partition_date", "yyyyMMdd")
            )
        )
    # Return union between one day on-top and multiple day on-top in daily aggregated
    # Kedro doesn't write output as hive table, which make it harder for us to manage partitioned table
    # We consider to manually save to hive table then update catalog accordingly

    # Since we are not using Kedro save function, we can return any spark DataFrame as unused memory dataset
    if not drop_replace_partition:
        output.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS prod_dataupsell."+hive_table)
        spark.sql("""CREATE TABLE prod_dataupsell."""+hive_table+"""
                    USING DELTA
                    PARTITIONED BY (partition_date)
                    AS
                    SELECT * FROM temp_view_load""")
    else:
        output.write.format("delta").mode("append").partitionBy(
            "partition_date"
        ).saveAsTable("prod_dataupsell." + hive_table)
    elapsed = format_time(time.time() - t0)
    logging.warning("Node create_daily_ontop_pack took: {:}".format(elapsed))
    return ontop_pack


def create_aggregate_ontop_package_preference_input(
    l1_data_ontop_purchase_daily: pyspark.sql.DataFrame,
    #unused_memory_dataset_1: pyspark.sql.DataFrame,
    aggregate_periods,
    hive_table,
    start_date=None,
    drop_replace_partition=False,
) -> pyspark.sql.DataFrame:
    """
    This function return weekly aggregated input for package preference creation.
    Features are aggregated according to provided aggregate_period,
    The features is grouped by customer identification keys, and each unique ontop package in weekly basis
    Args:
        l1_data_ontop_purchase_daily: normalized ontop input
        aggregate_periods: list of integer, number of days back from start of week to aggregate
        hive_table: hive table name
        start_date: date format to start aggregating data
        drop_replace_partition: Boolean, Perform drop partition before updating

    Returns:

    """
    import datetime
    t0 = time.time()
    spark = get_spark_session()
    if start_date is None:
        start_date = datetime.datetime.now() + datetime.timedelta(days=-14)
    end_date = datetime.datetime.now()
    # Drop partition for table update
    if drop_replace_partition:
        table = "prod_dataupsell." + hive_table
        partition_key = "start_of_week"
        drop_partition(start_date, end_date, table, partition_key)

    # List of week to update
    date_list = (
        l1_data_ontop_purchase_daily.where(
            "date(ontop_start_date) >= date('" + start_date.strftime("%Y-%m-%d") + "')"
        )
        .groupby("start_of_week")
        .agg(F.count("*").alias("CNT"))
        .select("start_of_week")
    )
    date_list.persist()

    # Collect pySpark DataFrame to python List
    aggregate_date_list = date_list.toPandas().values.tolist()

    # Aggregate weekly feature for each of the specify period
    i = True
    for week in aggregate_date_list:
        j = True
        for period in aggregate_periods:
            start_period = week[0] + datetime.timedelta(days=-period)
            aggregated_sdf = (
                l1_data_ontop_purchase_daily.where(
                    "partition_date > date('"
                    + start_period.strftime("%Y-%m-%d")
                    + "') AND partition_date <= date('"
                    + week[0].strftime("%Y-%m-%d")
                    + "')"
                )
                .groupby(
                    "analytic_id",
                    "old_subscription_identifier",
                    "access_method_num",
                    "register_date",
                    "promotion_code",
                    "price_inc_vat",
                    "package_type",
                    "package_group",
                    "mm_types",
                    "mm_data_type",
                    "mm_data_speed",
                    "package_name_report",
                    "data_quota_mb",
                    "duration",
                    "data_speed",
                )
                .agg(
                    F.sum("total_net_tariff").alias(
                        "total_spending_" + str(period) + "_days"
                    ),
                    F.count("*").alias("total_validity_" + str(period) + "_days"),
                    F.sum("data_sum").alias(
                        "total_data_volume_" + str(period) + "_days"
                    ),
                    F.sum("voice_call_out_duration_sum").alias(
                        "total_voice_call_out_duration_" + str(period) + "_days"
                    ),
                )
                .withColumn(
                    "average_data_volume_per_day_" + str(period) + "_days",
                    F.col("total_data_volume_" + str(period) + "_days")
                    / F.col("total_validity_" + str(period) + "_days"),
                )
                .withColumn(
                    "average_voice_volume_per_day_" + str(period) + "_days",
                    F.col("total_voice_call_out_duration_" + str(period) + "_days")
                    / F.col("total_validity_" + str(period) + "_days"),
                )
            )
            if j:
                spine_table = aggregated_sdf
            else:
                spine_table = spine_table.join(
                    aggregated_sdf,
                    [
                        "analytic_id",
                        "old_subscription_identifier",
                        "access_method_num",
                        "register_date",
                        "promotion_code",
                        "price_inc_vat",
                        "package_type",
                        "package_group",
                        "mm_types",
                        "mm_data_type",
                        "mm_data_speed",
                        "package_name_report",
                        "data_quota_mb",
                        "duration",
                        "data_speed",
                    ],
                    "left",
                )
            j = False
        if i:
            final_spine_table = spine_table.withColumn("start_of_week", F.lit(week[0]))
        else:
            final_spine_table = final_spine_table.union(
                spine_table.withColumn("start_of_week", F.lit(week[0]))
            )
        i = False
    # Manually save DataFrame to hive table
    if not drop_replace_partition:
        final_spine_table.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS prod_dataupsell."+hive_table)
        spark.sql("""CREATE TABLE prod_dataupsell."""+hive_table+"""
                    USING DELTA
                    PARTITIONED BY (start_of_week)
                    AS
                    SELECT * FROM temp_view_load""")
    else:
        final_spine_table.write.format("delta").mode("append").partitionBy(
            "start_of_week"
        ).saveAsTable("prod_dataupsell." + hive_table)
    elapsed = format_time(time.time() - t0)
    logging.warning("Node create_aggregate_ontop_package_preference_input took: {:}".format(elapsed))
    return final_spine_table


def create_ontop_package_preference(
    l4_data_ontop_purchase_week_hive_aggregate_feature: pyspark.sql.DataFrame,
    #unused_memory_dataset_2: pyspark.sql.DataFrame,
    aggregate_periods,
    hive_table: str,
    start_date=None,
    drop_replace_partition=False,
) -> pyspark.sql.DataFrame:
    """
    This function create package preference:
        Data on-top package preference, preference score is calculated by simple ranking score using 5 angles.
            - total_spending
            - total_validity
            - total_data_volume
            - average_data_volume_per_day
            - rank_data_speed
        The score is rank in the sense that, the more value within each axis mean the more customer tends to
        preferred to use that package over the period of time.
        All 5 axis are weighted equally.
        So after all axis has been ranked, they are all summed up and divided by 5.
        On-top package with lowest data_ontop_package_preference_score_X_days become preferred package for that period
    Args:
        l4_data_ontop_purchase_week_hive_aggregate_feature:
        aggregate_periods: list of integer, number of days back from start of week to aggregate
        hive_table: string of hive table name
        start_date: date format as the starting date for updating package preference
        drop_replace_partition: Boolean, to drop partition before updating table

    Returns:

    """
    import datetime
    spark = get_spark_session()
    t0 = time.time()
    end_date = datetime.datetime.now()
    if start_date is None:
        start_date = datetime.datetime.now() + datetime.timedelta(days=-7)
    if drop_replace_partition:
        table = "prod_dataupsell." + hive_table
        partition_key = "start_of_week"
        drop_partition(start_date, end_date, table, partition_key)
    l4_data_ontop_purchase_week_hive_aggregate_feature = l4_data_ontop_purchase_week_hive_aggregate_feature.where(
        "start_of_week >= date('" + start_date.strftime("%Y-%m-%d") + "')"
    )

    # Column of KPIs for aggregation, these KPIs exists in all given aggregate periods
    columns_to_aggregate = [
        "total_spending",
        "total_validity",
        "total_data_volume",
        "total_voice_call_out_duration",
        "average_data_volume_per_day",
        "average_voice_volume_per_day",
    ]

    # Expr for ranking, also pre-define on-top package's KPIs
    expr = [
        "*",
        "rank() OVER(PARTITION BY old_subscription_identifier,start_of_week ORDER BY data_speed DESC) AS rank_data_speed",
        "rank() OVER(PARTITION BY old_subscription_identifier,start_of_week ORDER BY data_quota_mb DESC) AS rank_data_quota_mb",
    ]

    # Use for loop to create ranking expr
    for period in aggregate_periods:
        for column in columns_to_aggregate:
            expr.append(
                "rank() OVER(PARTITION BY old_subscription_identifier,start_of_week ORDER BY "
                + column
                + "_"
                + str(period)
                + "_days"
                + " DESC) AS rank_"
                + column
                + "_"
                + str(period)
                + "_days"
            )

    l4_data_ontop_purchase_week_hive_aggregate_feature_rank = l4_data_ontop_purchase_week_hive_aggregate_feature.selectExpr(
        expr
    )

    # Create data on-top package score
    for period in aggregate_periods:
        l4_data_ontop_purchase_week_hive_aggregate_feature_rank = l4_data_ontop_purchase_week_hive_aggregate_feature_rank.withColumn(
            "data_ontop_package_preference_score_" + str(period) + "_days",
            (
                F.col("rank_total_spending_" + str(period) + "_days")
                + F.col("rank_total_validity_" + str(period) + "_days")
                + F.col("rank_total_data_volume_" + str(period) + "_days")
                + F.col("rank_average_data_volume_per_day_" + str(period) + "_days")
                + F.col("rank_data_speed")
            )
            / 5,
        )
    # List of columns to be select, we use first() to select these value as we already sort DataFrame by product score
    column_to_aggregate_groupby = [
        "promotion_code",
        "price_inc_vat",
        "package_type",
        "package_group",
        "mm_types",
        "mm_data_type",
        "mm_data_speed",
        "package_name_report",
        "data_quota_mb",
        "duration",
        "data_speed",
    ]

    i = True
    for period in aggregate_periods:
        group_by_aggregate_expr = [
            F.min("data_ontop_package_preference_score_" + str(period) + "_days").alias(
                "data_ontop_package_preference_score_" + str(period) + "_days"
            ),
        ]
        for column in column_to_aggregate_groupby:
            group_by_aggregate_expr.append(
                F.first(column).alias(column + "_" + str(period) + "_days")
            )
        tmp_output = (
            l4_data_ontop_purchase_week_hive_aggregate_feature_rank.sort(
                F.asc("data_ontop_package_preference_score_" + str(period) + "_days")
            )
            .groupby(
                "analytic_id",
                "old_subscription_identifier",
                "access_method_num",
                "register_date",
                "start_of_week",
            )
            .agg(*group_by_aggregate_expr)
        )
        if i:
            output_data_ontop = tmp_output
        else:
            output_data_ontop = output_data_ontop.join(
                tmp_output,
                [
                    "analytic_id",
                    "old_subscription_identifier",
                    "access_method_num",
                    "register_date",
                    "start_of_week",
                ],
                "left",
            )
        i = False

    # Save output table
    if not drop_replace_partition:
        output_data_ontop.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS prod_dataupsell."+hive_table)
        spark.sql("""CREATE TABLE prod_dataupsell."""+hive_table+"""
                    USING DELTA
                    PARTITIONED BY (start_of_week)
                    AS
                    SELECT * FROM temp_view_load""")
    else:
        output_data_ontop.write.format("delta").mode("append").partitionBy(
            "start_of_week"
        ).saveAsTable("prod_dataupsell." + hive_table)
    elapsed = format_time(time.time() - t0)
    logging.warning("Node create_ontop_package_preference took: {:}".format(elapsed))
    return output_data_ontop
