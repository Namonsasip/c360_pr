from pyspark.sql import SparkSession


def create_subscriber_tenure(input_df):

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    spark = SparkSession.builder.getOrCreate()

    df = spark.sql("""
    select access_method_num, 
           register_date, 
           if (tenure_temp < 0, 0, tenure_temp) as subscriber_tenure 
    from (
       select access_method_num,
              register_date, 
              months_between(to_date(cast(partition_month as STRING), 'yyyyMM'), to_date(register_date)) as tenure_temp 
       from input_table
       )
    """)

    return df