import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def joined_data_for_most_used_device(df1,df2,df3,df4):


    df = df1.join(df2, (df1.start_of_month == df2.start_of_month) &
                         (df1.start_of_week == df2.start_of_week) &
                         (df1.mobile_no == df2.mobile_no) &
                         (df1.register_date.eqNullSafe(df2.register_date)), 'inner')\
        .join(df3, (df1.start_of_month == df3.start_of_month) &
                         (df1.start_of_week == df3.start_of_week) &
                         (df1.mobile_no == df3.mobile_no) &
                         (df1.register_date.eqNullSafe(df3.register_date)), 'inner') \
        .join(df4, (df1.start_of_month == df4.start_of_month) &
              (df1.start_of_week == df4.start_of_week) &
              (df1.mobile_no == df4.mobile_no) &
              (df1.register_date.eqNullSafe(df4.register_date)), 'inner')

    last_two_week = ["most_used_handset_samsung_last_two_week","most_used_handset_huawei_last_two_week",
                              "most_used_handset_oppo_last_two_week","most_used_handset_apple_last_two_week",
                              "most_used_handset_xiaomi_last_two_week","most_used_handset_others_last_two_week"]

    last_month = ["most_used_handset_samsung_last_month","most_used_handset_huawei_last_month",
                              "most_used_handset_oppo_last_month","most_used_handset_apple_last_month",
                              "most_used_handset_xiaomi_last_month","most_used_handset_others_last_month"]

    last_three_month = ["most_used_handset_samsung_last_three_month","most_used_handset_huawei_last_three_month",
                              "most_used_handset_oppo_last_three_month","most_used_handset_apple_last_three_month",
                              "most_used_handset_xiaomi_last_three_month","most_used_handset_others_last_three_month"]

    output_df = df.select(df1["*"],
                          *last_two_week,
                          *last_month,
                          *last_three_month)

    return output_df