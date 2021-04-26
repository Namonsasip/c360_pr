from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from pyspark.sql import functions as f, DataFrame
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from pyspark.sql.types import *


def l1_billung_paym_detail(input_df,input_df2):
    spark = get_spark_session()
    input_df.createOrReplaceTempView("cpi")
    input_df2.createOrReplaceTempView('de')
    resultDF1 = spark.sql("""select 
    cpi.account_identifier,
    max(cpi.payment_date) as payment_date,
    case when cpi.no_of_days = 0 then 'On due' when cpi.no_of_days < 0 then 'Before due' else 'Over due' end as no_of_days,
    cpi.no_of_days as n_f_d,
    cpi.partition_date,
    cpi.PAYMENT_CHANNEL
    from cpi 
    where cpi.payment_date between CURRENT_DATE-30 and CURRENT_DATE
    group by cpi.account_identifier,cpi.partition_date,cpi.no_of_days,cpi.PAYMENT_CHANNEL""")
    resultDF1.createOrReplaceTempView('cc')
    resultDF3 = spark.sql(
        """select * ,Row_Number() OVER ( PARTITION BY account_identifier  order by partition_date desc ) as rownum 
        FROM cc""")
    resultDF3.createOrReplaceTempView('cef')
    resultDF4 = spark.sql("""select 
    cef.account_identifier,
    cef.payment_date,
    cef.no_of_days,
    cef.n_f_d,
    de.payment_channel_group,
    de.payment_channel_type,
    cef.partition_date
    from cef
    left join de on cef.PAYMENT_CHANNEL= de.payment_channel_code 
    where cef.rownum = '1' """)
    resultDF4.createOrReplaceTempView('wed')
    sqlStmt = """select distinct * from wed """
    df_output = spark.sql(sqlStmt)
    return df_output

def change_grouped_column_name(
        input_df,
        config
):
    df = node_from_config(input_df, config)
    for alias, col_name in config["rename_column"].items():
        df = df.withColumnRenamed(col_name, alias)

    return df


def dac_for_complaints_to_l1_pipeline(
        input_df: DataFrame,
        cust_df: DataFrame,
        target_table_name: str,
        exception_partiton_list=None):
    """
    :param input_df:
    :param cust_df:
    :param target_table_name:
    :param exception_partiton_list:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name,
                                                       exception_partitions = exception_partiton_list)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]
