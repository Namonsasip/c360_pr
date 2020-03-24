import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
#from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os


conf = os.environ["CONF"]

def massive_processing(input_df, customer_prof_input_df, priv_project, join_function,sql,partition_date, cust_partition_date, output_df_catalog):
    """
    :return:
    """
    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    cust_data_frame = customer_prof_input_df
    dates_list = cust_data_frame.select(f.to_date(cust_partition_date).alias(cust_partition_date)).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.to_date(partition_date).isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[curr_item]))
        joined_df = join_function(customer_prof_df,small_df,priv_project)
        output_df = node_from_config(joined_df, sql)
        if len(output_df.head(1)) == 0:
            print("Empty dataframe from node_from_config function's output for write")
        else:
            CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.to_date(partition_date).isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[first_item]))
    joined_df = join_function(customer_prof_df, return_df, priv_project)
    final_df = node_from_config(joined_df, sql)

    return final_df

def customize_massive_processing(input_df, customer_prof_input_df, priv_project, priv_points_raw,join_function,sql,partition_date, cust_partition_date, output_df_catalog):
    """
    :return:
    """
    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    cust_data_frame = customer_prof_input_df
    dates_list = cust_data_frame.select(f.to_date(cust_partition_date).alias(cust_partition_date)).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.to_date(partition_date).isin(*[curr_item]))
        small_priv_points = priv_points_raw.filter(f.to_date('tran_date').isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[curr_item]))
        joined_df = join_function(customer_prof_df,small_df,priv_project)
        joined_with_priv_points = daily_privilege_or_aunjai_data_with_priv_points(joined_df,small_priv_points)
        output_df = node_from_config(joined_with_priv_points, sql)
        if len(output_df.head(1)) == 0:
            print("Empty dataframe from node_from_config function's output for write")
        else:
            CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.to_date(partition_date).isin(*[first_item]))
    small_priv_points = priv_points_raw.filter(f.to_date('tran_date').isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[first_item]))
    joined_df = join_function(customer_prof_df, return_df, priv_project)
    joined_with_priv_points = daily_privilege_or_aunjai_data_with_priv_points(joined_df, small_priv_points)
    final_df = node_from_config(joined_with_priv_points, sql)

    return final_df


def priv_project_ranked(priv_project):

    priv_project = priv_project.select("project_id",
                                           "category",
                                           "start_date",
                                           "stop_date",
                                           "project_type_id",
                                           "project_subtype")

    window = Window.partitionBy("project_id")\
        .orderBy(f.col("start_date").desc(),
                 f.col("stop_date").desc())

    priv_project = priv_project.withColumn("rank",f.row_number().over(window))\
        .where("rank = 1")

    return priv_project

def daily_privilege_or_aunjai_data_with_customer_profile(customer_prof,privilege_or_aunjai_data,priv_project):


    output_df_1 = customer_prof.join(privilege_or_aunjai_data,(customer_prof.access_method_num == privilege_or_aunjai_data.mobile_no) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(privilege_or_aunjai_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(privilege_or_aunjai_data.response_date)),'left')

    output_df_1 = output_df_1.drop(privilege_or_aunjai_data.register_date)

    output_df_2 = output_df_1.join(priv_project,
                               output_df_1.project_id.cast(StringType()) == priv_project.project_id.cast(StringType()),'left')

    output_df = output_df_2.drop(output_df_1.project_id)\
        .drop(output_df_1.category)

    return output_df


def daily_privilege_or_aunjai_data_with_customer_profile_reward_and_points_spend(customer_prof,privilege_or_aunjai_data,priv_project):


    output_df_1 = customer_prof.join(privilege_or_aunjai_data,(customer_prof.access_method_num == privilege_or_aunjai_data.mobile_no) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(privilege_or_aunjai_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(privilege_or_aunjai_data.response_date)))

    # if len(output_df_1.head(1)) == 0:
    #         print("Empty_output_1 from customer and aunjai join")
    #         return output_df_1
    # else:
    output_df_1 = output_df_1.drop(privilege_or_aunjai_data.register_date)

    output_df_2 = output_df_1.join(priv_project,
                               output_df_1.project_id.cast(StringType()) == priv_project.project_id.cast(StringType()))

    output_df = output_df_2.drop(output_df_1.project_id)\
            .drop(output_df_1.category)

    return output_df


def daily_privilege_or_aunjai_data_with_priv_points(privilege_or_aunjai_data_with_customer_profile,priv_points_raw):

    privilege_or_aunjai_data = privilege_or_aunjai_data_with_customer_profile.select("access_method_num",
                                         "subscription_identifier",
                                         "register_date",
                                         "event_partition_date",
                                         "start_of_week",
                                         "start_of_month",
                                         "project_id",
                                         "msg_id",
                                         "project_type_id",
                                         "project_subtype",
                                         "msg_event_id",
                                         "category",
                                         "response_date")\
        .where("project_type_id = 6 and project_subtype like 'REDEEM%' and msg_event_id = 13")

    priv_points = priv_points_raw.where("point_tran_type_id in (15,35) and refund_session_id is null")\
        .groupBy("msisdn","msg_id","project_id",
                 f.to_date("tran_date").alias("tran_date"))\
        .agg(f.sum("points").alias("loyalty_points_spend"))

    output_df_1 = privilege_or_aunjai_data.join(priv_points,
                                   (privilege_or_aunjai_data.access_method_num == priv_points.msisdn) &
                                   (privilege_or_aunjai_data.msg_id.cast(StringType()).eqNullSafe(priv_points.msg_id.cast(StringType()))) &
                                   (privilege_or_aunjai_data.project_id.cast(StringType()) == priv_points.project_id.cast(StringType())) &
                                   (privilege_or_aunjai_data.event_partition_date == f.to_date(priv_points.tran_date)),'left')

    output_df = output_df_1.drop(priv_points.project_id)\
        .drop(priv_points.msg_id)

    return output_df

def loyalty_number_of_services_for_each_category(customer_prof,input_df,priv_project, sql):
    """
    :return:
    """

    priv_project = priv_project_ranked(priv_project)

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "start_of_week",
                                         "start_of_month")

    return_df = massive_processing(input_df, customer_prof, priv_project, daily_privilege_or_aunjai_data_with_customer_profile,
                                   sql,'response_date', 'event_partition_date',"l1_loyalty_number_of_services")
    return return_df

def loyalty_number_of_rewards_for_each_category(customer_prof,input_df,priv_project, sql):
    """
    :return:
    """

    priv_project = priv_project_ranked(priv_project)
    priv_project = priv_project.where("project_type_id = 6 and project_subtype like 'REDEEM%'")

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "start_of_week",
                                         "start_of_month")

    customer_prof = customer_prof.where("event_partition_date is not null")

    input_df = input_df.where("msg_event_id = 13")

    return_df = massive_processing(input_df, customer_prof, priv_project, daily_privilege_or_aunjai_data_with_customer_profile_reward_and_points_spend,
                                   sql,'response_date', 'event_partition_date',"l1_loyalty_number_of_rewards")

    if len(return_df.head(1)) == 0:
        print("Empty dataframe from loyalty_number_of_rewards_for_each_category funtion's output for write")
    else:
        return return_df

def loyalty_number_of_points_spend_for_each_category(customer_prof,input_df,priv_project, priv_points_raw, sql):
    """
    :return:
    """

    priv_project = priv_project_ranked(priv_project)
    priv_project = priv_project.where("project_type_id = 6 and project_subtype like 'REDEEM%'")

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "start_of_week",
                                         "start_of_month")
    customer_prof = customer_prof.where("event_partition_date is not null")

    input_df = input_df.where("msg_event_id = 13")

    return_df = customize_massive_processing(input_df, customer_prof, priv_project, priv_points_raw, daily_privilege_or_aunjai_data_with_customer_profile_reward_and_points_spend,
                                   sql,'response_date','event_partition_date',"l1_loyalty_number_of_points_spend")

    if len(return_df.head(1)) == 0:
        print("Empty dataframe from loyalty_number_of_points_spend_for_each_category funtion's output for write")
    else:
        return return_df


    #return return_df


