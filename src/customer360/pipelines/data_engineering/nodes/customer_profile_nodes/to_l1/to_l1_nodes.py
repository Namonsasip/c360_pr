from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, union_dataframes_with_missing_cols

from pyspark.sql import DataFrame, functions as f
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import get_max_date_from_master_data, node_from_config
import os

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

    os.environ["partition_date_filter"] = str(min_value)

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
    spark = get_spark_session()
    product_offering = get_max_date_from_master_data(product_offering, 'partition_date')
    product_drm_resenade_package = get_max_date_from_master_data(product_drm_resenade_package, 'partition_date')
    product_ru_m_mkt_promo_group = get_max_date_from_master_data(product_ru_m_mkt_promo_group, 'partition_date')
    product_pru_m_package = get_max_date_from_master_data(product_pru_m_package, 'partition_date')
    profile_same_id_card = get_max_date_from_master_data(profile_same_id_card, 'partition_month')

    profile_union_daily.createOrReplaceTempView("profile_union_daily")
    profile_mnp.createOrReplaceTempView("profile_mnp")
    product_offering.createOrReplaceTempView("product_offering")
    product_offering_pps.createOrReplaceTempView("product_offering_pps")
    profile_same_id_card.createOrReplaceTempView("profile_same_id_card")
    product_drm_resenade_package.createOrReplaceTempView("product_drm_resenade_package")
    product_ru_m_mkt_promo_group.createOrReplaceTempView("product_ru_m_mkt_promo_group")
    product_pru_m_package.createOrReplaceTempView("product_pru_m_package")

    # sql = """
    # select a.*
    #       ,b.recipient_conso as previous_mnp_port_out_oper_name
    #       ,b.port_order_status_date as previous_mnp_port_out_date
    # from profile_union_daily a
    # left join
    # (
    #    select *
    #    from
    #    (
    #      select * ,ROW_NUMBER() OVER(PARTITION BY access_method_num, identification_num ORDER BY port_order_status_date desc) as row
    #      from profile_mnp
    #      where port_sub_type is null and port_type_cd = 'Port - Out'
    #      and port_order_status_cd in ('Completed','Complete','Deactivated')
    #    ) rn
    #    where row = 1
    # ) b on a.access_method_num = b.access_method_num and a.national_id_card=b.identification_num
    # """

    sql = """
    select a.*
          ,o.recipient_conso as previous_mnp_port_out_oper_name
          ,o.port_order_status_date as previous_mnp_port_out_date
          ,case when a.charge_type = 'Pre-paid' or a.charge_type = 'Post-paid' then
                case when o.recipient_conso is not null then 'Y' else 'N' end 
            else null end as previous_mnp_port_out_yn
          ,i.donor_conso as previous_mnp_port_in_oper_name
          ,i.port_order_status_date as previous_mnp_port_in_date
          ,case when a.charge_type = 'Pre-paid' or a.charge_type = 'Post-paid' then
                case when i.donor_conso is not null then 'Y' else 'N' end 
            else null end as previous_mnp_port_in_yn
    from profile_union_daily a 
    left join profile_mnp o
    on a.access_method_num = o.access_method_num and a.national_id_card=o.identification_num
    and o.port_type_cd = 'Port - Out' 
    left join profile_mnp i
    on a.access_method_num = i.access_method_num and a.national_id_card=i.identification_num
    and i.port_type_cd = 'Port - In'
    """
    df = spark.sql(sql)
    #df = df.filter("row = 1").drop("row")  ## remove because filter in query

    # previous_mnp_port_out_yn
    # df.createOrReplaceTempView("df")
    # sql = """
    # select *,
    # case when charge_type = 'Pre-paid' or charge_type = 'Post-paid' then
    #         case when previous_mnp_port_out_oper_name is not null then 'Y' else 'N' end
    #      else null end as previous_mnp_port_out_yn
    # from df
    # """
    # df = spark.sql(sql)

    # previous_mnp_port_in_oper_namea/previous_mnp_port_in_date
    # df.createOrReplaceTempView("df")
    # sql = """
    # select a.*,b.donor_conso as previous_mnp_port_in_oper_name,b.port_order_status_date as previous_mnp_port_in_date
    # from df a
    # left join
    # (select *
    #  from
    #   (
    #       select * ,ROW_NUMBER() OVER(PARTITION BY access_method_num,identification_num ORDER BY port_order_status_date desc) as row
    #       from profile_mnp
    #       where port_type_cd = 'Port - In'
    #       and port_sub_type is null and port_order_status_cd in ('Completed','Complete','Deactivated')
    #   ) a
    #   where row = 1
    # ) b on a.access_method_num = b.access_method_num and a.national_id_card=b.identification_num
    # """
    # df = spark.sql(sql)
    #df = df.filter("row = 1").drop("row") ## remove because filter in query

    # previous_mnp_port_in_yn
    # df.createOrReplaceTempView("df")
    # sql = """
    # select *,case when charge_type = 'Pre-paid' or charge_type = 'Post-paid' then
    # case when previous_mnp_port_in_oper_name is not null then 'Y' else 'N' end else null end as previous_mnp_port_in_yn
    # from df
    # """
    # df = spark.sql(sql)

    # current_promotion_code
    df.createOrReplaceTempView("df")
    product_offering_pps_1 = product_offering_pps.select("offering_cd").distinct()
    product_offering_pps_1.createOrReplaceTempView("product_offering_pps_1")
    sql = """ select a.*,case when a.charge_type = 'Pre-paid' and a.current_promotion_code_temp is null
    then b.offering_cd else a.current_promotion_code_temp end as current_promotion_code
    from(select a.*,(case when a.charge_type = 'Pre-paid' then c.offering_cd else b.offering_cd end) as current_promotion_code_temp
    from df a
    left join product_offering b on a.current_package_id = b.offering_id
    left join product_offering_pps_1 c on a.current_package_id = c.offering_cd) a 
    left join product_offering b on a.current_package_id = b.offering_id"""
    df = spark.sql(sql)
    df = df.drop("current_promotion_code_temp")

    # card_type
    df.createOrReplaceTempView("df")
    sql = """
    select a.*,case when a.charge_type = 'Pre-paid' then a.card_type_desc else b.card_no_description end as card_type
    from df a
    left join (
    select sub_id,card_no,card_no_description from
    (select sub_id,card_no,card_no_description,ROW_NUMBER() OVER(PARTITION BY sub_id,card_no,card_no_description ORDER BY register_date desc) as row 
    from profile_same_id_card) acc where row = 1) b
    on a.old_subscription_identifier = b.sub_id and a.national_id_card=b.card_no """ # remove month_id from partition by
    df = spark.sql(sql)
    df = df.drop("card_type_desc")

    # serenade_package_type
    df.createOrReplaceTempView("df")
    sql = """
    select a.*,case when a.charge_type = 'Pre-paid' then null else b.package_type end as serenade_package_type
    from df a
    left join product_drm_resenade_package b on a.current_package_id = b.offering_id
    """
    df = spark.sql(sql)

    # promotion_group
    df.createOrReplaceTempView("df")
    sql = """
    select a.*,case when a.charge_type = 'Pre-paid' then (
    case when c.promotion_group_tariff = 'Smartphone & Data Package' then 'VOICE+VAS' 
    when c.promotion_group_tariff = 'Net SIM' then 'VAS'else 'VOICE' end)else b.service_group end as promotion_group
    from df a
    left join product_ru_m_mkt_promo_group b on a.current_package_id = b.offering_id
    left join product_pru_m_package c on a.current_package_id = c.offering_id
    """
    df = spark.sql(sql)
    return df

def add_feature_lot5(
    active_sub_summary_detail: DataFrame,
    profile_union_daily_feature: DataFrame
) -> DataFrame:

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([active_sub_summary_detail, profile_union_daily_feature]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    # max_date = active_sub_summary_detail.select(
    #                 f.max(f.col("date_id")).alias("max_date")).collect()[0].max_date
    #
    # active_sub_summary_detail = active_sub_summary_detail.filter(f.col("date_id") == max_date)

    active_sub_summary_detail = get_max_date_from_master_data(active_sub_summary_detail, 'partition_date')

    profile_union_daily_feature.createOrReplaceTempView('union_daily_feature')
    active_sub_summary_detail.createOrReplaceTempView('sub_summary_detail')

    spark = get_spark_session()
    # sql_l5 = """
    # select a.*,
    #    b.installation_tumbol_th as installation_tumbol_th,
    #    b.installation_amphur_th as installation_amphur_th,
    #    b.installation_province_cd as installation_province_cd,
    #    b.installation_province_en as installation_province_en,
    #    b.installation_province_th as installation_province_th,
    #    b.installation_region as installation_region,
    #    b.installation_sub_region as installation_sub_region,
    #    b.cmd_channel_type as registration_channel
    # from union_daily_feature a
    # left join sub_summary_detail b on a.old_subscription_identifier = b.c360_subscription_identifier
    # and b.date_id = (select max(date_id) from sub_summary_detail)
    # """

    sql_l5 = """
    select a.*,
       b.installation_tumbol_th as installation_tumbol_th,
       b.installation_amphur_th as installation_amphur_th,
       b.installation_province_cd as installation_province_cd,
       b.installation_province_en as installation_province_en,
       b.installation_province_th as installation_province_th,
       b.installation_region as installation_region,
       b.installation_sub_region as installation_sub_region,
       b.cmd_channel_type as registration_channel
    from union_daily_feature a
    left join sub_summary_detail b on a.old_subscription_identifier = b.c360_subscription_identifier 
    """
    df = spark.sql(sql_l5)

    return df

def def_feature_lot7(
        df_union,
        df_service_post,
        df_service_pre,
        df_cm_t_newsub,
        df_iden
):
    partition_date_filter = '20210611'    #os.getenv("partition_date_filter", None)
    spark = get_spark_session()
    df_service_post = df_service_post.filter(f.col("partition_date") <= int(partition_date_filter))
    df_service_pre = df_service_pre.filter(f.col("partition_date") <= int(partition_date_filter))

    # df_service_post = df_service_post.filter(f.col("partition_date") <= int(partition_date_filter))
    # df_service_pre = df_service_pre.filter(f.col("partition_date") <= int(partition_date_filter))
    #
    df_union.createOrReplaceTempView("df_union")
    df_service_post.createOrReplaceTempView("df_service_post")
    df_service_pre.createOrReplaceTempView("df_service_pre")
    df_cm_t_newsub.createOrReplaceTempView("df_cm_t_newsub")
    df_iden.createOrReplaceTempView("df_iden")

    #2 location_activation_group
    sql="""
    select *,
    (case when charge_type = 'Pre-paid' then (case when activate_province_cd in ('BKK' ,'BKK-E') then 'City'
    when activate_province_cd is null then null else 'UPC' end)
    else (case when province_cd in ( 'BKK' ,'BKK-E')  then 'City' when amphur like '%เมือง%' then 'City'
    when amphur in ('Muang Amnat Charoen','Muang Ang Thong','Phra Nakhon Sri Ayutthaya','Muang Bung Kan','Muang Buri Ram','Muang Chachoengsao','Muang Chai Nat','Muang Chaiyaphum','Muang Chanthaburi','Muang Chiang Mai','Muang Chiang Rai','Muang Chon Buri','Muang Chumphon','Muang Kalasin','Muang Kamphaeng Phet','Muang Kanchanaburi','Muang Khon Kaen','Muang Krabi','Muang Lampang','Muang Lamphun','Muang Loei','Muang Lop Buri','Muang Mae Hong Son','Muang Maha Sarakham','Muang Muddahan','Muang Nakhon Nayok','Muang Nakhon Pathom','Muang Nakhon Ratchasima','Muang Nakhon Phanom','Muang Nakhon Sawan','Muang Nakhon Sri Thammarat','Muang Nan','Muang Narathiwat','Muang Nong Khai','Muang Nong Bua Lam Phu','Muang Nonthaburi','Muang Pathum Thani','Muang Pattani','Muang Phangnga','Muang Phatthalung','Muang Phayao','Muang Phetchabun','Muang Phetchaburi','Muang Phichit','Muang Phitsanulok','Muang Phrae','Muang Phuket','Muang Prachin Buri','Muang Ranong','Muang Ratchaburi','Muang Prachaubkirikhan','Muang Rayong','Muang Roi Et','Muang Sa Kaeo','Muang Sakon Nakhon','Muang Samut Prakarn','Muang Samut Sakhon','Muang Saraburi','Muang Samut Songkhram','Muang Satun','Muang Si Sa Ket','Muang Sing Buri','Muang Songkhla','Muang Sukhothai','Muang Suphanburi','Muang Surat Thani','Muang Surin','Muang Tak','Muang Trang','Muang Trat','Muang Ubon Ratchathani','Muang Udon Thani','Muang Uthai Thani','Muang Uttaradit','Muang Yala','Muang Ya Sothon') then 'City'
    else (case when province_cd is null then null else 'UPC' end)end)end) as location_activation_group
    from df_union
    """
    df_union = spark.sql(sql)

    # 3 #4 latest_convert  / convert_date
    df_union.createOrReplaceTempView("df_union")
        # 1 POST_RANK
    # sql = """
    # select mobile_num as mobile_no,register_dt as register_date,service_order_submit_dt as convert_date
    # ,case when charge_type = 'Pre-paid' then 'Post2Pre'
    # when charge_type = 'Post-paid' then 'Pre2Post' end as latest_convert
    # from (
    # select mobile_num,register_dt,service_order_submit_dt,charge_type,ROW_NUMBER() OVER(PARTITION BY mobile_num ORDER BY service_order_submit_dt desc,service_order_created_dttm desc,register_dt desc) as row
    # from df_service_post where unique_order_flag = 'Y' and service_order_type_cd = 'Change Charge Type'
    # ) where row = 1
    # """

    sql = """
    select mobile_no ,register_date, convert_date, latest_convert
    from (
    select mobile_no,register_date,convert_date,convert_type as latest_convert
          ,ROW_NUMBER() OVER(PARTITION BY mobile_no ORDER BY convert_date desc,order_create_date desc,register_date desc) as row
    from df_service_post 
    ) where row = 1
    """
    df_service_post_rank = spark.sql(sql)
    df_service_post_rank.createOrReplaceTempView("df_service_post_rank")

        # 2 PRE_RANK
    # sql = """select mobile_no,register_date,order_dt as convert_date
    # ,case when order_type in ('Port By Nature (Convert Post -> Pre)', 'Return Mobile No(Convert Post -> Pre)') then 'Post2Pre'
    # when order_type in ('Port by Nature (Convert Pre -> Post)', 'Return Mobile No(Convert Pre -> Post)')  then 'Pre2Post' end as latest_convert
    # from (select mobile_no,register_date,order_dt,order_type
    # ,ROW_NUMBER() OVER(PARTITION BY mobile_no ORDER BY order_dt desc,register_date desc) as row
    # from df_service_pre where order_type in ('Port By Nature (Convert Post -> Pre)','Port by Nature (Convert Pre -> Post)'
    # ,'Return Mobile No(Convert Post -> Pre)','Return Mobile No(Convert Pre -> Post)')) where row = 1"""

    sql = """
    select mobile_no,register_date,convert_date, latest_convert
    from (select mobile_no,register_date,convert_date,convert_type as latest_convert
    ,ROW_NUMBER() OVER(PARTITION BY mobile_no ORDER BY convert_date desc,register_date desc) as row
    from df_service_pre ) where row = 1
    """
    df_service_pre = spark.sql(sql)
    df_service_pre.createOrReplaceTempView("df_service_pre")

        # 3 UNION_RANKING
    sql = """select mobile_no,register_date,convert_date,latest_convert,check from(
    select *,ROW_NUMBER() OVER(PARTITION BY mobile_no ORDER BY convert_date desc,register_date desc,check asc) as row from(
    select mobile_no,register_date,convert_date,latest_convert,"df_service_pre" as check from df_service_pre
    union all
    select mobile_no,register_date,convert_date,latest_convert,"df_service_post" as check from df_service_post_rank)
    )where row =1"""
    df_service_pre_post = spark.sql(sql)
    df_service_pre_post.createOrReplaceTempView("df_service_pre_post")

        # 4 df_union_join_first
    sql = """
    select a.*
    ,b.latest_convert
    ,b.convert_date
    ,b.check
    from df_union a
    left join df_service_pre_post b
    on a.access_method_num = b.mobile_no and a.register_date = b.register_date
    """
    df_union_re = spark.sql(sql)
    df_union_re.createOrReplaceTempView("df_union_re")

        # 5 Find_union_join
    df_union_re_con = df_union_re.where(
        "(latest_convert = 'Post2Pre' and charge_type = 'Post-paid') or (latest_convert = 'Pre2Post' and charge_type = 'Pre-paid') and check = 'df_service_post'")
    df_union_re_con.createOrReplaceTempView("df_union_re_con")

        # 6 Find_union_join_df_service_post_flag
    # sql = """select * from df_service_post where service_order_type_cd = "Change Charge Type" and unique_order_flag = "Y" """
    # df_service_post_flag = spark.sql(sql)
    # df_service_post_flag.createOrReplaceTempView("df_service_post_flag")
    df_service_post_flag = df_service_post
    df_service_post_flag.createOrReplaceTempView("df_service_post_flag")

        # 7 df_union_inner_join
    # sql = """select a.mobile_num,a.register_dt,a.charge_type,a.service_order_submit_dt,a.service_order_created_dttm from df_service_post_flag a
    # inner join df_union_re_con b
    # on a.mobile_num = b.access_method_num
    # and a.register_dt = b.register_date"""
    sql = """select a.mobile_no as mobile_num,a.register_date as register_dt
    ,case when convert_type = 'Pre2Post' then 'Post-paid' when convert_type = 'Post2Pre' then 'Pre-paid' end as change_type
    ,a.convert_date as service_order_submit_dt,a.order_create_date as service_order_created_dttm 
    from df_service_post_flag a
    inner join df_union_re_con b
    on a.mobile_no = b.access_method_num
    and a.register_date = b.register_date"""
    df_union_inner_join = spark.sql(sql)
    df_union_inner_join.createOrReplaceTempView("df_union_inner_join")

        # 8 Switch_MissMatch_Pre2Post_Post2Pre_service_post
    sql = """select * from(
    select a.mobile_num,a.register_dt,count(*) as cnt
    from (
    select * from(select *,ROW_NUMBER() OVER(PARTITION BY mobile_num ORDER BY service_order_submit_dt desc,register_dt desc) as row from df_union_inner_join) where row = 1) a
    inner join df_union_inner_join b
    on b.mobile_num = a.mobile_num
    and b.register_dt = a.register_dt
    and a.service_order_submit_dt = b.service_order_submit_dt
    group by 1,2) where cnt > 1
    """
    df_service_inner_join = spark.sql(sql)
    df_service_inner_join.createOrReplaceTempView("df_service_inner_join")

        # 9 df_union_join_final_join
    sql = """
    select a.*,
    (case when (a.latest_convert = 'Post2Pre' and a.charge_type = 'Post-paid') or (a.latest_convert = 'Pre2Post' and a.charge_type = 'Pre-paid')
      then
      (case
         when a.check = 'df_service_pre' then null
         when a.check = 'df_service_post' then
         (case
           when c.mobile_num is not null then
             (case when a.charge_type = 'Pre-paid' then 'Post2Pre'
             when a.charge_type = 'Post-paid' then 'Pre2Post' end)
           else null end)
         else null end)
       else a.latest_convert end) as latest_convert_re
    ,(case when (a.latest_convert = 'Post2Pre' and a.charge_type = 'Post-paid') or (a.latest_convert = 'Pre2Post' and a.charge_type = 'Pre-paid')
      then
      (case
         when a.check = 'df_service_pre' then null
         when a.check = 'df_service_post' then
         (case
           when c.mobile_num is not null then b.convert_date
           else null end)
         else null end)
       else a.convert_date end) as convert_date_re
    from df_union_re a
    left join df_service_pre_post b
    on a.access_method_num = b.mobile_no
    and a.register_date = b.register_date
    left join df_service_inner_join c
    on a.access_method_num = c.mobile_num
    and a.register_date = c.register_dt
    """
    df_union = spark.sql(sql)
    df_union = df_union.drop("convert_date").drop("latest_convert").drop("check")
    df_union = df_union.withColumnRenamed("convert_date_re", "convert_date").withColumnRenamed("latest_convert_re", "latest_convert")

    # 5 acquisition_location_code
    df_union.createOrReplaceTempView("df_union")
    sql = """
    select a.*
    ,b.report_location_loc as acquisition_location_code
    from df_union a
    left join (select c360_subscription_identifier,report_location_loc from (select c360_subscription_identifier,report_location_loc
    ,ROW_NUMBER() OVER(PARTITION BY c360_subscription_identifier ORDER BY partition_month desc) as row from df_cm_t_newsub
    where order_status like 'Complete%' and order_type not in ('New Registration - Prospect','Change Service','Change SIM')) where row = 1) b
    on a.old_subscription_identifier = b.c360_subscription_identifier and a.charge_type = 'Post-paid'
    """
    df_union = spark.sql(sql)

    # 6 service_month_on_charge_type
    df_union.createOrReplaceTempView("df_union")
    sql = """
    select *,case when convert_date is not null then year(to_date('"""+partition_date_filter+"""', 'yyyyMMdd'))*12 - year(convert_date)*12 + month(to_date('"""+partition_date_filter+"""','yyyyMMdd')) - month(convert_date) 
    else subscriber_tenure_month end as service_month_on_charge_type    from df_union
    """
    df_union = spark.sql(sql)

    # 7 prepaid_identification_YN
    df_union.createOrReplaceTempView("df_union")
    # sql = """
    # select a.*,
    # case when a.charge_type = 'Pre-paid' then (
    # case when COALESCE(b.mobile_no,c.access_method_num ) is not null then 'Y' else 'N' end) else null end as prepaid_identification_yn
    # from df_union a
    # left join (select distinct mobile_no from df_hist where prepaid_identn_end_dt > "9999-12-31") b
    # on a.access_method_num = b.mobile_no
    # left join (select distinct access_method_num from df_iden where new_prepaid_identn_id is null) c
    # on a.access_method_num = c.access_method_num
    # """
    sql = """
    select a.*,
    case when a.charge_type = 'Pre-paid' then (
    case when b.mobile_no is not null then 'Y' else 'N' end) else null end as prepaid_identification_yn
    from df_union a
    left join df_iden b
    on a.access_method_num = c.access_method_num
    """
    df_union = spark.sql(sql)
    return df_union

def test_order_change_charge_type(
        df_service_post,
        # data_dic
):
    if check_empty_dfs([df_service_post]):
        return get_spark_empty_df()

    # df_service_post = df_service_post.where("partition_date<=20200101")
    df_service_post = df_service_post.where("unique_order_flag = 'Y' and service_order_type_cd = 'Change Charge Type'")
    df_service_post = df_service_post.withColumnRenamed("mobile_num", "mobile_no")\
                                     .withColumnRenamed("register_dt", "register_date") \
                                     .withColumnRenamed("service_order_submit_dt", "convert_date") \
                                     .withColumnRenamed("service_order_created_dttm", "order_create_date") \
                                     .withColumn("convert_type",\
                                     f.expr("case when charge_type = 'Pre-paid' then 'Post2Pre'\
                                                  when charge_type = 'Post-paid' then 'Pre2Post' end"))\
                                     .withColumn('event_partition_date',f.expr("to_date(cast(partition_date as STRING), 'yyyyMMdd')"))
    result_df = df_service_post.select('mobile_no', 'register_date', 'convert_date', 'order_create_date', 'convert_type','event_partition_date','partition_date')
    # result_df = node_from_config(df_service_post, data_dic)
    return result_df

def test_order_change_charge_type_pre(
        df_service_pre,
        # data_dic
):
    if check_empty_dfs([df_service_pre]):
        return get_spark_empty_df()

    # df_service_post = df_service_post.where("partition_date<=20200101")
    df_service_pre = df_service_pre.where("order_type in ('Port By Nature (Convert Post -> Pre)'\
                                          ,'Port by Nature (Convert Pre -> Post)'\
                                          ,'Return Mobile No(Convert Post -> Pre)'\
                                          ,'Return Mobile No(Convert Pre -> Post)')")
    df_service_pre = df_service_pre.withColumnRenamed("order_dt", "convert_date") \
                                     .withColumn("convert_type",\
                                     f.expr("case when order_type in ('Port By Nature (Convert Post -> Pre)', 'Return Mobile No(Convert Post -> Pre)') then 'Post2Pre'\
                                     when order_type in ('Port by Nature (Convert Pre -> Post)', 'Return Mobile No(Convert Pre -> Post)')  then 'Pre2Post' end"))\
                                     .withColumn('event_partition_date',f.expr("to_date(cast(partition_date as STRING), 'yyyyMMdd')"))
    result_df = df_service_pre.select('mobile_no', 'register_date', 'convert_date', 'convert_type','event_partition_date','partition_date')
    # result_df = node_from_config(df_service_post, data_dic)
    return result_df


def test_mnp_order(
        df_mnp_order,
        # data_dic

):
    if check_empty_dfs([df_mnp_order]):
        return get_spark_empty_df()

    df_mnp_order = df_mnp_order.where("port_sub_type is null and port_order_status_cd in ('Completed','Complete','Deactivated')")\
                          .withColumn("rn", f.expr("row_number() over (partition by access_method_num,identification_num,port_type_cd order by port_order_status_date desc)"))\
                          .where("rn = 1")\
                          .select('access_method_num', 'identification_num', 'donor_conso', 'recipient_conso', 'port_type_cd', 'port_order_status_date')
    result_df = df_mnp_order
    return result_df

def test_prepaid_iden(
        df_hist,
        df_iden
):

    df_iden.createOrReplaceTempView("df_iden")
    df_hist.createOrReplaceTempView("df_hist")
    spark = get_spark_session()
    sql="""    
    select distinct mobile_no as access_method_numfrom df_hist where prepaid_identn_end_dt > "9999-12-31"
    union 
    select distinct access_method_num from df_iden where new_prepaid_identn_id is null
    """
    result_df = spark.sql(sql)
    return result_df