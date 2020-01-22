from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType


def union_daily_cust_profile(
        cust_pre,
        cust_post,
        cust_non_mobile,
        column_to_extract
):

    def setup_df_with_column_to_extract(df, key):
        columns = []

        for alias, each_col in column_to_extract[key].items():

            if each_col is None:
                df = df.withColumn(alias, F.lit(None).cast(StringType()))
                columns.append(alias)
                continue

            columns.append(F.col(each_col).alias(alias))

        return df.select(columns)

    df = setup_df_with_column_to_extract(cust_pre, "customer_pre") \
        .union(setup_df_with_column_to_extract(cust_post, 'customer_post')) \
        .union(setup_df_with_column_to_extract(cust_non_mobile, 'customer_non_mobile'))

    return df


def merge_union_and_basic_features(union_features, basic_features):
    union_features.createOrReplaceTempView("union_features")
    basic_features.createOrReplaceTempView("basic_features")

    spark = SparkSession.builder.getOrCreate()

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

