from pyspark.sql import SparkSession


def union_daily_cust_profile(
        cust_pre,
        cust_post,
        cust_non_mobile,
        column_to_extract
):
    columns_map ={
        "customer_pre": [
            "subscription_identifier",
            "mobile_no",
            "activation_date"
        ],
        "customer_post": [
            "subscription_identifier",
            "mobile_no",
            "register_date"
        ],
        "customer_non_mobile": [
            "subscription_id",
            "mobile_no",
            "activation_date"
        ]
    }

    for each_table_columns in columns_map.values():
        each_table_columns.extend(column_to_extract)

    df = cust_pre.select(columns_map['customer_pre']) \
        .union(cust_post.select(columns_map['customer_post'])) \
        .union(cust_non_mobile.select(columns_map['customer_non_mobile']))

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

