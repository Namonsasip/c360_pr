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


def add_feature_profile_with_join_table(
        profile_union_daily,
        profile_mnp,
        product_offering,
        product_offering_pps,
        profile_same_id_card,
        product_drm_resenade_package,
        product_ru_m_mkt_promo_group,
        product_pru_m_package
):
    profile_union_daily.createOrReplaceTempView("profile_union_daily")
    profile_mnp.createOrReplaceTempView("profile_mnp")
    product_offering.createOrReplaceTempView("product_offering")
    product_offering_pps.createOrReplaceTempView("product_offering_pps")
    profile_same_id_card.createOrReplaceTempView("profile_same_id_card")
    product_drm_resenade_package.createOrReplaceTempView("product_drm_resenade_package")
    product_ru_m_mkt_promo_group.createOrReplaceTempView("product_ru_m_mkt_promo_group")
    product_pru_m_package.createOrReplaceTempView("product_pru_m_package")

    # previous_mnp_port_out_oper_namea/ previous_mnp_port_out_date
    sql = """
    select a.*,
    b.recipient_conso as previous_mnp_port_out_oper_name,b.port_order_status_date as previous_mnp_port_out_date
    ,ROW_NUMBER() OVER(PARTITION BY a.access_method_num,a.national_id_card ORDER BY b.port_order_status_date desc) as row 
    from profile_union_daily a left join (
    select * from profile_mnp where port_sub_type is null and port_type_cd = 'Port - Out' 
    and port_order_status_cd in ('Completed','Complete','Deactivated')
    ) b on a.access_method_num = b.access_method_num and a.national_id_card=b.identification_num
    """
    df = spark.sql(sql)
    df = df.filter("row = 1").drop("row")

    # previous_mnp_port_out_yn
    df.createOrReplaceTempView("df")
    sql = """
    select *,
    case when charge_type = 'Pre-paid' or charge_type = 'Post-paid' then 
    case when previous_mnp_port_out_oper_name is not null then 'Y' else 'N' end else null end as previous_mnp_port_out_yn 
    from df
    """
    df = spark.sql(sql)

    # previous_mnp_port_in_oper_namea/previous_mnp_port_in_date
    df.createOrReplaceTempView("df")
    sql = """  
    select a.*,b.donor_conso as previous_mnp_port_in_oper_name,b.port_order_status_date as previous_mnp_port_in_date
    ,ROW_NUMBER() OVER(PARTITION BY a.access_method_num,a.national_id_card ORDER BY b.port_order_status_date desc) as row 
    from df a
    left join (select * from profile_mnp where port_type_cd = 'Port - In' 
    and port_sub_type is null and port_order_status_cd in ('Completed','Complete','Deactivated')
    ) b     on a.access_method_num = b.access_method_num and a.national_id_card=b.identification_num
    """
    df = spark.sql(sql)
    df = df.filter("row = 1").drop("row")

    # previous_mnp_port_in_yn
    df.createOrReplaceTempView("df")
    sql = """
    select *,case when charge_type = 'Pre-paid' or charge_type = 'Post-paid' then 
    case when previous_mnp_port_in_oper_name is not null then 'Y' else 'N' end else null end as previous_mnp_port_in_yn 
    from df
    """
    df = spark.sql(sql)

    # current_promotion_code
    df.createOrReplaceTempView("df")
    p_script = """ls /dbfs/mnt/customer360-blob-data/C360/PRODUCT/product_offering | sort -u | awk -F'=' '{if(length($2) == 8) print $2}' |tail -1"""
    product_offering_max_date = str(subprocess.check_output(p_script, shell=True).splitlines()).split("'")[1]

    product_offering_pps_1 = product_offering_pps.select("offering_cd").distinct()
    product_offering_pps_1.createOrReplaceTempView("product_offering_pps_1")
    sql = """ select a.*,case when a.charge_type = 'Pre-paid' and a.current_promotion_code_temp is null then b.offering_cd else a.current_promotion_code_temp end as current_promotion_code
    from(select a.*,(case when a.charge_type = 'Pre-paid' then c.offering_cd else b.offering_cd end) as current_promotion_code_temp
    from df a
    left join product_offering b on a.current_package_id = b.offering_id and b.partition_date = """ + product_offering_max_date + """
    left join product_offering_pps_1 c on a.current_package_id = c.offering_cd) a """
    df = spark.sql(sql)

    # card_type
    df.createOrReplaceTempView("df")

    p_script = """ls /dbfs/mnt/customer360-blob-data/C360/PROFILE/profile_ru_t_mobile_same_id_card | sort -u | awk -F'=' '{if(length($2) == 6) print $2}' |tail -1"""
    profile_same_id_card_max_date = str(subprocess.check_output(p_script, shell=True).splitlines()).split("'")[1]
    sql = """  
    select a.*,case when a.charge_type = 'Pre-paid' then a.card_type_desc else b.card_no end as card_type 
    from df a 
    left join (
    select sub_id,card_no from(
    select sub_id,card_no,ROW_NUMBER() OVER(PARTITION BY sub_id,card_no,month_id ORDER BY register_date desc) as row 
    from profile_same_id_card where partition_month=""" + profile_same_id_card_max_date + """) acc where row = 1) b 
    on a.subscription_identifier = b.sub_id and a.national_id_card=b.card_no """
    df = spark.sql(sql)

    # serenade_package_type
    df.createOrReplaceTempView("df")
    p_script = """ls /dbfs/mnt/customer360-blob-data/C360/PRODUCT/product_drm_resenade_package_master | sort -u | awk -F'=' '{if(length($2) == 8) print $2}' |tail -1"""
    product_drm_resenade_package_max_date = str(subprocess.check_output(p_script, shell=True).splitlines()).split("'")[1]
    sql = """  
    select a.*,case when a.charge_type = 'Pre-paid' then null else b.package_type end as serenade_package_type
    from df a
    left join product_drm_resenade_package b on a.current_package_id = b.offering_id 
    and b.partition_date = """ + product_drm_resenade_package_max_date + """
    """
    df = spark.sql(sql)

    # promotion_group
    df.createOrReplaceTempView("df")

    p_script = """ls /dbfs/mnt/customer360-blob-data/C360/PRODUCT/product_ru_m_mkt_promo_group_master | sort -u | awk -F'=' '{if(length($2) == 8) print $2}' |tail -1"""
    product_ru_m_mkt_promo_group_max_date = str(subprocess.check_output(p_script, shell=True).splitlines()).split("'")[1]
    p_script = """ls /dbfs/mnt/customer360-blob-data/C360/PRODUCT/product_pru_m_package_master_group | sort -u | awk -F'=' '{if(length($2) == 8) print $2}' |tail -1"""
    product_pru_m_package_max_date = str(subprocess.check_output(p_script, shell=True).splitlines()).split("'")[1]
    sql = """  
    select a.*,case when a.charge_type = 'Pre-paid' then (
    case when c.promotion_group_tariff = 'Smartphone & Data Package' then 'VOICE+VAS' when c.promotion_group_tariff = 'Net SIM' then 'VAS'else 'VOICE' end
    )else b.service_group end as promotion_group 
    from df a
    left join product_ru_m_mkt_promo_group b on a.current_package_id = b.offering_id and b.partition_date=""" + product_ru_m_mkt_promo_group_max_date + """
    left join product_pru_m_package c on a.current_package_id = c.offering_id and c.partition_date = """ + product_pru_m_package_max_date + """
    """
    df = spark.sql(sql)
    return df