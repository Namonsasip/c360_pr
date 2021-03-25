from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, union_dataframes_with_missing_cols

from pyspark.sql import DataFrame, functions as f


def union_daily_cust_profile(
        cust_pre,
        cust_post,
        cust_non_mobile,
        column_to_extract
):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cust_pre, cust_post, cust_non_mobile]):
        return get_spark_empty_df()

    cust_pre = data_non_availability_and_missing_check(df=cust_pre, grouping="daily",
                                                       par_col="partition_date",
                                                       target_table_name="l1_customer_profile_union_daily_feature")

    cust_post = data_non_availability_and_missing_check(df=cust_post, grouping="daily",
                                                        par_col="partition_date",
                                                        target_table_name="l1_customer_profile_union_daily_feature")

    cust_non_mobile = data_non_availability_and_missing_check(df=cust_non_mobile, grouping="daily",
                                                              par_col="partition_date",
                                                              target_table_name="l1_customer_profile_union_daily_feature")

    if check_empty_dfs([cust_pre, cust_post, cust_non_mobile]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            cust_pre.select(
                f.max(f.col("partition_date")).alias("max_date")),
            cust_post.select(
                f.max(f.col("partition_date")).alias("max_date")),
            cust_non_mobile.select(
                f.max(f.col("partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    cust_pre = cust_pre.filter(f.col("partition_date") <= min_value)

    cust_post = cust_post.filter(f.col("partition_date") <= min_value)

    cust_non_mobile = cust_non_mobile.filter(f.col("partition_date") <= min_value)

    # Getting unique data from pre-paid
    cust_pre = cust_pre.withColumn("rn", f.expr(
        "row_number() over(partition by mobile_no,partition_date order by register_date desc)"))
    cust_pre = cust_pre.where("rn = 1").drop("rn")

    # Getting unique data from post_paid
    cust_post = cust_post.withColumn("rn", f.expr(
        "row_number() over(partition by mobile_no,partition_date order by mobile_status_date desc)"))
    cust_post = cust_post.where("rn = 1").drop("rn")

    # Getting unique data from non_mobile
    cust_non_mobile = cust_non_mobile.withColumn("rn", f.expr(
        "row_number() over(partition by mobile_no,partition_date order by mobile_status_date desc)"))
    cust_non_mobile = cust_non_mobile.where("rn = 1").drop("rn")

    cust_pre.createOrReplaceTempView("cust_pre")
    cust_post.createOrReplaceTempView("cust_post")
    cust_non_mobile.createOrReplaceTempView("cust_non_mobile")

    sql_stmt = """
            select {cust_pre_columns} from cust_pre
            union all
            select {cust_post_columns} from cust_post
            union all
            select {cust_non_mobile_columns} from cust_non_mobile
    """

    def setup_column_to_extract(key):
        columns = []

        for alias, each_col in column_to_extract[key].items():
            columns.append("{} as {}".format(each_col, alias))

        return ','.join(columns)

    sql_stmt = sql_stmt.format(cust_pre_columns=setup_column_to_extract("customer_pre"),
                               cust_post_columns=setup_column_to_extract("customer_post"),
                               cust_non_mobile_columns=setup_column_to_extract("customer_non_mobile"))
    spark = get_spark_session()
    df = spark.sql(sql_stmt)

    # Getting unique records from combined data
    df = df.withColumn("rn", f.expr(
        "row_number() over(partition by access_method_num,partition_date order by register_date desc, mobile_status_date desc )"))
    df = df.where("rn = 1").drop("rn")

    return df


def generate_modified_subscription_identifier(
        cust_profile_df: DataFrame
) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cust_profile_df]):
        return get_spark_empty_df()

    cust_profile_df = (cust_profile_df
                       .withColumn("subscription_identifier",
                                   f.expr("case when lower(charge_type) = 'pre-paid' then "
                                          "concat(access_method_num, '-', date_format(register_date, 'yyyyMMdd')) "
                                          "else old_subscription_identifier end")))

    return cust_profile_df



