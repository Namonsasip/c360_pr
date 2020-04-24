import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.config_parser import node_from_config

conf = os.getenv("CONF", None)


def build_loyalty_number_of_services_weekly(l1_loyalty_number_of_services_daily: DataFrame,
                                            l0_loyalty_priv_project: DataFrame,
                                            l0_loyalty_priv_category: DataFrame,
                                            l2_loyalty_number_of_services_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_number_of_services_daily:
    :param l0_loyalty_priv_project:
    :param l0_loyalty_priv_category:
    :param l2_loyalty_number_of_services_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_number_of_services_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_number_of_services_daily, grouping="daily",
                                                       par_col="partition_date",
                                                       target_table_name="l2_loyalty_number_of_services_weekly")

    if check_empty_dfs([l1_loyalty_number_of_services_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    win_category = Window.partitionBy("category_id").orderBy(f.col("partition_date").desc())
    win_project = Window.partitionBy("project_id").orderBy(f.col("start_date").desc(), f.col("stop_date").desc())

    l0_loyalty_priv_category = l0_loyalty_priv_category.withColumn("rnk", f.row_number().over(win_category)) \
        .filter(f.col("rnk") == 1) \
        .select("category_id", f.col("category").alias("category_text"))

    l0_loyalty_priv_project = l0_loyalty_priv_project.withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="left")

    return_df = node_from_config(return_df, l2_loyalty_number_of_services_weekly)

    return return_df


def build_loyalty_number_of_rewards_redeemed_weekly(l1_loyalty_number_of_rewards_redeemed_daily: DataFrame,
                                                    l0_loyalty_priv_project: DataFrame,
                                                    l0_loyalty_priv_category: DataFrame,
                                                    l2_loyalty_number_of_rewards_redeemed_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_number_of_rewards_redeemed_daily:
    :param l0_loyalty_priv_project:
    :param l0_loyalty_priv_category:
    :param l2_loyalty_number_of_services_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_number_of_rewards_redeemed_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_number_of_rewards_redeemed_daily, grouping="daily",
                                                       par_col="partition_date",
                                                       target_table_name="l2_loyalty_number_of_services_weekly")

    if check_empty_dfs([l1_loyalty_number_of_rewards_redeemed_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    win_category = Window.partitionBy("category_id").orderBy(f.col("partition_date").desc())
    win_project = Window.partitionBy("project_id").orderBy(f.col("start_date").desc(), f.col("stop_date").desc())

    l0_loyalty_priv_category = l0_loyalty_priv_category.withColumn("rnk", f.row_number().over(win_category)) \
        .filter(f.col("rnk") == 1) \
        .select("category_id", f.col("category").alias("category_text"))

    l0_loyalty_priv_project = l0_loyalty_priv_project.where("project_type_id = 6 and "
                                                            "lower(project_subtype) like '%redeem%'")\
        .withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="inner")

    return_df = node_from_config(return_df, l2_loyalty_number_of_rewards_redeemed_weekly)

    return return_df

# def massive_processing(input_df, customer_prof_input_df, join_function,sql,partition_date, cust_partition_date, output_df_catalog):
#     """
#     :return:
#     """
#     def divide_chunks(l, n):
#
#         # looping till length l
#         for i in range(0, len(l), n):
#             yield l[i:i + n]
#
#     CNTX = load_context(Path.cwd(), env=conf)
#     data_frame = input_df
#     cust_data_frame = customer_prof_input_df
#     dates_list = cust_data_frame.select(f.to_date(cust_partition_date).alias(cust_partition_date)).distinct().collect()
#     mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
#     logging.info("Dates to run for {0}".format(str(mvv_array)))
#
#     mvv_new = list(divide_chunks(mvv_array, 1))
#     add_list = mvv_new
#
#     first_item = add_list[0]
#
#     add_list.remove(first_item)
#     for curr_item in add_list:
#         logging.info("running for dates {0}".format(str(curr_item)))
#         small_df = data_frame.filter(f.to_date(partition_date).isin(*[curr_item]))
#         customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[curr_item]))
#         joined_df = join_function(customer_prof_df,small_df)
#         output_df = node_from_config(joined_df, sql)
#         CNTX.catalog.save(output_df_catalog, output_df)
#
#     logging.info("Final date to run for {0}".format(str(first_item)))
#     return_df = data_frame.filter(F.to_date(partition_date).isin(*[first_item]))
#     customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[first_item]))
#     joined_df = join_function(customer_prof_df, return_df)
#     final_df = node_from_config(joined_df, sql)
#
#     return final_df
#
# def priv_customer_profile_joined(customer_prof,input_df):
#
#     output_df = customer_prof.join(input_df,(customer_prof.access_method_num == input_df.msisdn) &
#                                    (customer_prof.register_date.eqNullSafe(f.to_date(input_df.register_date))) &
#                                    (customer_prof.start_of_week == input_df.start_of_week),'left')
#
#     output_df = output_df.drop(input_df.start_of_week)\
#         .drop(input_df.register_date)
#
#     return output_df
#
# def loyalty_serenade_class(input_df, customer_prof, sql):
#     """
#     :return:
#     """
#     customer_prof = customer_prof.select("access_method_num",
#                                          "subscription_identifier",
#                                          f.to_date("register_date").alias("register_date"),
#                                          "start_of_week",
#                                          "charge_type")
#     customer_prof = customer_prof.dropDuplicates(["start_of_week","access_method_num","register_date","subscription_identifier"])
#
#     input_df = input_df.withColumn("tran_date",f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))
#     input_df = input_df.withColumn("start_of_week",f.to_date(f.date_trunc("week","tran_date")))
#
#     return_df = massive_processing(input_df, customer_prof, priv_customer_profile_joined, sql,'start_of_week', 'start_of_week',"l2_loyalty_serenade_class")
#     return return_df
#
