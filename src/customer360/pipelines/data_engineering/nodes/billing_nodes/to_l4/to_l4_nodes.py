import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def joined_data_for_popular_topup_day(df1,df2,df3,df4):


    df = df1.join(df2, (df1.start_of_month == df2.start_of_month) &
                         (df1.start_of_week == df2.start_of_week) &
                         (df1.access_method_num == df2.access_method_num) &
                         (df1.register_date.eqNullSafe(df2.register_date)), 'inner')\
        .join(df3, (df1.start_of_month == df3.start_of_month) &
                         (df1.start_of_week == df3.start_of_week) &
                         (df1.access_method_num == df3.access_method_num) &
                         (df1.register_date.eqNullSafe(df3.register_date)), 'inner') \
        .join(df4, (df1.start_of_month == df4.start_of_month) &
              (df1.start_of_week == df4.start_of_week) &
              (df1.access_method_num == df4.access_method_num) &
              (df1.register_date.eqNullSafe(df4.register_date)), 'inner')

    output_df = df.select(df1["*"],
                          df2["payment_popular_day_last_two_week"],
                          df3["payment_popular_day_last_month"],
                          df4["payment_popular_day_last_three_month"])

    return output_df

def joined_data_for_popular_topup_hour(df1,df2,df3,df4):


    df = df1.join(df2, (df1.start_of_month == df2.start_of_month) &
                         (df1.start_of_week == df2.start_of_week) &
                         (df1.access_method_num == df2.access_method_num) &
                         (df1.register_date.eqNullSafe(df2.register_date)), 'inner')\
        .join(df3, (df1.start_of_month == df3.start_of_month) &
                         (df1.start_of_week == df3.start_of_week) &
                         (df1.access_method_num == df3.access_method_num) &
                         (df1.register_date.eqNullSafe(df3.register_date)), 'inner') \
        .join(df4, (df1.start_of_month == df4.start_of_month) &
              (df1.start_of_week == df4.start_of_week) &
              (df1.access_method_num == df4.access_method_num) &
              (df1.register_date.eqNullSafe(df4.register_date)), 'inner')

    output_df = df.select(df1["*"],
                          df2["payment_popular_hour_last_two_week"],
                          df3["payment_popular_hour_last_month"],
                          df4["payment_popular_hour_last_three_month"])

    return output_df

def joined_data_for_most_popular_topup_channel(df1,df2,df3,df4):


    df = df1.join(df2, (df1.start_of_month == df2.start_of_month) &
                         (df1.start_of_week == df2.start_of_week) &
                         (df1.access_method_num == df2.access_method_num) &
                         (df1.register_date.eqNullSafe(df2.register_date)), 'inner')\
        .join(df3, (df1.start_of_month == df3.start_of_month) &
                         (df1.start_of_week == df3.start_of_week) &
                         (df1.access_method_num == df3.access_method_num) &
                         (df1.register_date.eqNullSafe(df3.register_date)), 'inner') \
        .join(df4, (df1.start_of_month == df4.start_of_month) &
              (df1.start_of_week == df4.start_of_week) &
              (df1.access_method_num == df4.access_method_num) &
              (df1.register_date.eqNullSafe(df4.register_date)), 'inner')

    output_df = df.select(df1["*"],
                          df2["payment_most_popular_topup_channel_last_two_week"],
                          df3["payment_most_popular_topup_channel_last_month"],
                          df4["payment_most_popular_topup_channel_last_three_month"])

    return output_df


