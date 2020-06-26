
import logging, os
from pyspark.sql import functions as f
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.config_parser import node_from_config
from pyspark.sql import DataFrame


conf = os.getenv("CONF", None)


def sale_product_customer_master_on_top_features(sale_df: DataFrame,
                                 product_df: DataFrame,
                                 customer_df: DataFrame,
                                 volume_feature_dict,
                                 name_feature_dict,
                                 exception_partitions_list) -> DataFrame:
    """

    :param sale_df:
    :param product_df:
    :param customer_df:
    :param volume_feature_dict:
    :param name_feature_dict:
    :param exception_partitions_list:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([sale_df, product_df, customer_df]):
        return get_spark_empty_df()

    sale_df = data_non_availability_and_missing_check(
        df=sale_df,
        grouping="weekly", par_col="partition_date",
        target_table_name="l2_sales_number_and_volume_on_top_transaction_weekly",
        missing_data_check_flg='Y',
        exception_partitions = exception_partitions_list)

    product_df = data_non_availability_and_missing_check(
        df=product_df,
        grouping="weekly", par_col="partition_date",
        target_table_name="l2_sales_number_and_volume_on_top_transaction_weekly")

    customer_df = data_non_availability_and_missing_check(
        df=customer_df,
        grouping="weekly", par_col="start_of_week",
        target_table_name="l2_sales_number_and_volume_on_top_transaction_weekly")

    min_value = union_dataframes_with_missing_cols(
        [
            sale_df.select(
                f.max(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd')))).alias("max_date")),
            product_df.select(
                f.max(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd')))).alias("max_date")),
            customer_df.select(
                f.max(f.col("start_of_week")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    sale_df = sale_df.filter(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    product_df = product_df.filter(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    customer_df = customer_df.filter(f.col("start_of_week") <= min_value)

    if check_empty_dfs([sale_df, product_df, customer_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    sale_cols = ['access_method_num', 'partition_date', 'cmd_channel_type',
                 'cmd_channel_sub_type', 'promotion_price_amount', 'offering_title', 'offering_code', 'event_start_dttm']
    product_cols = ['partition_date', 'package_type', 'promotion_code']

    sale_product_join_cols = ['start_of_week', 'offering_code']

    sale_df = sale_df.select(sale_cols)
    sale_df = sale_df.withColumn("start_of_week", f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))))
    sale_df = sale_df.drop("partition_date")

    product_df = product_df.select(product_cols)
    product_df = product_df.withColumn("start_of_week", f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))))
    product_df = product_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,promotion_code order by start_of_week desc)"))
    product_df = product_df.where("rn = 1").drop("rn")
    product_df = product_df.withColumnRenamed("promotion_code", "offering_code").drop("partition_date")


    #joining sales and product
    sale_product_master_df = sale_df.join(product_df, sale_product_join_cols, how='left')

    #creating volume features
    master_volume_features = node_from_config(sale_product_master_df, volume_feature_dict)

    #creating name features
    master_name_features_temp = node_from_config(sale_product_master_df, name_feature_dict)
    master_name_features_temp = master_name_features_temp.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,access_method_num order by start_of_week desc)"))
    master_name_features = master_name_features_temp.where("rn = 1")
    master_name_features = master_name_features.drop("rn")

    master_sales_features_join_cols = ['start_of_week', 'access_method_num']

    #joining volume and name feature to create one master feature table
    master_sales_features = master_volume_features.join(master_name_features, master_sales_features_join_cols, how='left')

    customer_cols = ['access_method_num',
                     'subscription_identifier',
                     'national_id_card',
                     'start_of_week']

    cust_join_cols = ['start_of_week', 'access_method_num']

    customer_df = customer_df.select(customer_cols)
    customer_df = customer_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,access_method_num order by start_of_week desc)"))
    customer_df = customer_df.where("rn = 1")
    customer_df = customer_df.drop("rn")

    master_df = master_sales_features.join(customer_df, cust_join_cols, how='left')

    return master_df


def sale_product_customer_master_main_features(sale_df: DataFrame,
                                 product_df: DataFrame,
                                 customer_df: DataFrame,
                                 volume_feature_dict,
                                 name_feature_dict,
                                 exception_partitions_list) -> DataFrame:
    """

    :param sale_df:
    :param product_df:
    :param customer_df:
    :param volume_feature_dict:
    :param name_feature_dict:
    :param exception_partitions_list:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([sale_df, product_df, customer_df]):
        return get_spark_empty_df()

    sale_df = data_non_availability_and_missing_check(
        df=sale_df,
        grouping="weekly", par_col="partition_date",
        target_table_name="l2_sales_number_and_volume_main_transaction_weekly",
        missing_data_check_flg='Y',
        exception_partitions=exception_partitions_list)

    product_df = data_non_availability_and_missing_check(
        df=product_df,
        grouping="weekly", par_col="partition_date",
        target_table_name="l2_sales_number_and_volume_main_transaction_weekly")

    customer_df = data_non_availability_and_missing_check(
        df=customer_df,
        grouping="weekly", par_col="start_of_week",
        target_table_name="l2_sales_number_and_volume_main_transaction_weekly")

    min_value = union_dataframes_with_missing_cols(
        [
            sale_df.select(
                f.max(f.to_date(
                    f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd')))).alias(
                    "max_date")),
            product_df.select(
                f.max(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd')))).alias("max_date")),
            customer_df.select(
                f.max(f.col("start_of_week")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    sale_df = sale_df.filter(
        f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    product_df = product_df.filter(f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    customer_df = customer_df.filter(f.col("start_of_week") <= min_value)

    if check_empty_dfs([sale_df, product_df, customer_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    sale_cols = ['service_order_item_id', 'partition_date', 'service_order_item_eff_dttm',
                 'offering_title', 'offering_cd', 'offering_promotion_class', 'mobile_num', 'crm_subscription_id', 'offering_price', 'cmd_channel_type', 'cmd_channel_sub_type']
    product_cols = ['partition_date', 'service_group', 'promotion_code']

    sale_product_join_cols = ['start_of_week', 'offering_code']

    #select only main package
    sale_df = sale_df.select(sale_cols).where("lower(offering_promotion_class) = 'main'")
    sale_df = sale_df.withColumn("start_of_week", f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))))
    sale_df = sale_df.withColumnRenamed("offering_cd", "offering_code").drop("partition_date") \
                    .withColumnRenamed("mobile_num","access_method_num") \
                    .withColumnRenamed("crm_subscription_id","subscription_identifier")

    
    product_df = product_df.select(product_cols)
    product_df = product_df.withColumn("start_of_week", f.to_date(f.date_trunc("week", f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))))
    product_df = product_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,promotion_code order by start_of_week desc)"))
    product_df = product_df.where("rn = 1").drop("rn")
    product_df = product_df.withColumnRenamed("promotion_code", "offering_code").drop("partition_date")
    

    #joining sales and product
    sale_product_master_df = sale_df.join(product_df, sale_product_join_cols, how='left')

    #creating volume features
    master_volume_features = node_from_config(sale_product_master_df, volume_feature_dict)
    
    #creating name features
    master_name_features_temp = node_from_config(sale_product_master_df, name_feature_dict)
    master_name_features_temp = master_name_features_temp.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,access_method_num,subscription_identifier order by start_of_week desc)"))
    master_name_features = master_name_features_temp.where("rn = 1")
    master_name_features = master_name_features.drop("rn")

    master_sales_features_join_cols = ['start_of_week','access_method_num', 'subscription_identifier']

    #joining volume and name feature to create one master feature table
    master_sales_features = master_volume_features.join(master_name_features, master_sales_features_join_cols, how='left')

    customer_cols = ['access_method_num',
                     'subscription_identifier',
                     'national_id_card',
                     'start_of_week']

    cust_join_cols = ['start_of_week', 'access_method_num','subscription_identifier']

    customer_df = customer_df.select(customer_cols)
    customer_df = customer_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,access_method_num,subscription_identifier order by start_of_week desc)"))
    customer_df = customer_df.where("rn = 1")
    customer_df = customer_df.drop("rn")

    master_df = master_sales_features.join(customer_df, cust_join_cols, how='left')

    return master_df



