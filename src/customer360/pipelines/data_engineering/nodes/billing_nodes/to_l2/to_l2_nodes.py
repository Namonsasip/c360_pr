import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window



def top_up_channel_joined_data(input_df,topup_type_ref):

    output_df = input_df.join(topup_type_ref,input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,'left')

    return output_df




