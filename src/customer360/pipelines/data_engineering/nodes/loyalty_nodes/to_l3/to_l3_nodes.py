from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l2.to_l2_nodes import massive_processing
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
import os

# conf = os.environ["CONF"]

def priv_customer_profile_joined(customer_prof,input_df):

    output_df = customer_prof.join(input_df,(customer_prof.access_method_num == input_df.msisdn) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(input_df.register_date))) &
                                   (customer_prof.start_of_month == input_df.start_of_month),'left')

    output_df = output_df.drop(input_df.start_of_month)\
        .drop(input_df.register_date)

    return output_df

def loyalty_serenade_class(input_df, customer_prof, sql):
    """
    :return:
    """
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "partition_month",
                                         "charge_type")\
        .withColumnRenamed("partition_month","start_of_month")

    input_df = input_df.withColumn("tran_date",f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd'))
    input_df = input_df.withColumn("start_of_month",f.to_date(f.date_trunc("month","tran_date")))

    return_df = massive_processing(input_df, customer_prof, priv_customer_profile_joined, sql,'start_of_month', 'start_of_month',"l3_loyalty_serenade_class")
    return return_df