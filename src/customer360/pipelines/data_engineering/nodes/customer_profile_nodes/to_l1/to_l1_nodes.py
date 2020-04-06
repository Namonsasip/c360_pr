from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df


def union_daily_cust_profile(
        cust_pre,
        cust_post,
        cust_non_mobile,
        column_to_extract
):
    if len(cust_pre.head(1)) == 0 and len(cust_post.head(1)) == 0 and len(cust_non_mobile.head(1)) == 0:
        return get_spark_empty_df()

    cust_pre.createOrReplaceTempView("cust_pre")
    cust_post.createOrReplaceTempView("cust_post")
    cust_non_mobile.createOrReplaceTempView("cust_non_mobile")

    sql_stmt = """
        with unioned_cust_profile as (
            select {cust_pre_columns} from cust_pre
            union all
            select {cust_post_columns} from cust_post
            union all
            select {cust_non_mobile_columns} from cust_non_mobile
        ),
        dedup_amn_reg_date as (
            select unioned_cust_profile.access_method_num, 
                    max(unioned_cust_profile.register_date) as register_date,
                    unioned_cust_profile.partition_date
            from unioned_cust_profile
            inner join (
              select access_method_num, 
                     max(register_date) as register_date,
                     partition_date
              from unioned_cust_profile
              group by access_method_num, partition_date
            ) t
            on unioned_cust_profile.access_method_num = t.access_method_num
               and unioned_cust_profile.register_date = t.register_date
               and unioned_cust_profile.partition_date = t.partition_date
            group by unioned_cust_profile.access_method_num, 
                     unioned_cust_profile.partition_date
            having count(unioned_cust_profile.access_method_num, 
                         unioned_cust_profile.partition_date) = 1
        )
        select unioned_cust_profile.*
        from unioned_cust_profile
        inner join dedup_amn_reg_date d
        on unioned_cust_profile.access_method_num = d.access_method_num
            and unioned_cust_profile.register_date = d.register_date
            and unioned_cust_profile.partition_date = d.partition_date
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
