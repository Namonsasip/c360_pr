import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *



def daily_privilege_or_aunjai_data_with_customer_profile(customer_prof,privilege_or_aunjai_data,priv_project):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    priv_project = priv_project.select("project_id",
                                           "category",
                                           "start_date",
                                           "stop_date",
                                           "project_type_id",
                                           "project_subtype")

    customer_prof = customer_prof.withColumn("start_of_month",
                                             f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",
                    f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    output_df_1 = customer_prof.join(privilege_or_aunjai_data,(customer_prof.access_method_num == privilege_or_aunjai_data.mobile_no) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(privilege_or_aunjai_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(privilege_or_aunjai_data.response_date)),'left')

    output_df_1 = output_df_1.drop(privilege_or_aunjai_data.register_date)

    window = Window.partitionBy("project_id")\
        .orderBy(f.col("start_date").desc(),
                 f.col("stop_date").desc())

    priv_project = priv_project.withColumn("rank",f.row_number().over(window))\
        .where("rank = 1")

    output_df_2 = output_df_1.join(priv_project,
                               output_df_1.project_id.cast(StringType()) == priv_project.project_id.cast(StringType()),'left')

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