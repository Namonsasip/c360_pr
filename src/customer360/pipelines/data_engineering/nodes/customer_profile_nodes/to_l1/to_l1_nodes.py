from src.customer360.utilities.spark_util import get_spark_session


def union_daily_cust_profile(
        cust_pre,
        cust_post,
        cust_non_mobile,
        column_to_extract
):
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

    return df
