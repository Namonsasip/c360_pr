from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as f

def previous_device_with_configuration(df):

    spark = SparkSession.builder.getOrCreate()
    df.createOrReplaceTempView("df")

    output_df = spark.sql(""" with table_with_unique_device as 
    (select start_of_month,
            mobile_no,
            register_date,
            handset_imei,
            handset_brand_code,
            handset_model_code,
            handset_brand_name,
            handset_model_name, 
            max(device_last_use_time) over 
            (partition by mobile_no,
                          register_date,
                          handset_imei,
                          handset_brand_code,
                          handset_model_code,
                          handset_brand_name,
                          handset_model_name
                          order by cast(cast(start_of_month as timestamp) as long) asc
                          range between 90 * 24 * 60 * 60 preceding and current row) as device_last_use_time_in_three_months
    from df),

    table_with_previous_device_ranks as 
    (select start_of_month,
            mobile_no,
            register_date,
            handset_imei,
            handset_brand_name,
            device_last_use_time_in_three_months,
            rank() over 
            (partition by mobile_no,
                          register_date,
                          start_of_month
                          order by device_last_use_time_in_three_months desc) as rank
    from table_with_unique_device),
    
    table_with_previous_device as 
    (select start_of_month,
            mobile_no,
            register_date,
            handset_imei,
            handset_brand_name,
            device_last_use_time_in_three_months
    from table_with_previous_device_ranks where rank<=2)
    
    select start_of_month,
           mobile_no,
           register_date,
           last_value(handset_imei) over 
           (partition by mobile_no,
                          register_date,
                          start_of_month
                          order by device_last_use_time_in_three_months desc) as previous_device_imei,
           last_value(handset_brand_name) over 
           (partition by mobile_no,
                          register_date,
                          start_of_month
                          order by device_last_use_time_in_three_months desc) as previous_device_brand
     from table_with_previous_device """)

    return output_df