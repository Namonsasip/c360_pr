import pyspark.sql.functions as f ,logging
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
import pyspark as pyspark
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from typing import Dict, Any
from functools import reduce


   ################################# customer_app_category_windows ###############################
def customer_app_category_windows (df_input: DataFrame,groupby: Dict[str, Any],Column_df: Dict[str, Any],) -> DataFrame :
    spark = get_spark_session()

    if check_empty_dfs([df_input]):
        return get_spark_empty_df()

    #filter month
    mobile_app_last_month =  df_input.filter(f.date_trunc("month", f.col("start_of_month")) == f.date_trunc("month", f.add_months(f.current_date(), -1))).limit(1)
    mobile_app_last_month.createOrReplaceTempView("input_last_month")
    mobile_app_last_3_month =  df_input.filter(f.date_trunc("month", f.col("start_of_month")) >= f.date_trunc("month", f.add_months(f.current_date(), -3))).limit(1)
    mobile_app_last_3_month.createOrReplaceTempView("input_last_three_month")
    #last month
    P_SQL_last_month = "SELECT "
    for i in groupby:
        P_SQL_last_month = P_SQL_last_month+i+","
    for i in Column_df:
        P_SQL_last_month = P_SQL_last_month+"max("+i+") as max_"+i+"_last_month,"
        P_SQL_last_month = P_SQL_last_month+"min("+i+") as min_"+i+"_last_month,"
        P_SQL_last_month = P_SQL_last_month+"avg("+i+") as avg_"+i+"_last_month,"
        P_SQL_last_month = P_SQL_last_month+"std("+i+") as avg_"+i+"_last_month,"

    P_SQL_last_month = P_SQL_last_month[:-1] +" from input_last_month "
    P_SQL_last_month = P_SQL_last_month + "group by "
    for i in groupby:
        P_SQL_last_month = P_SQL_last_month+i+","
    P_SQL_last_month = P_SQL_last_month[:-1]
    output_last_month = spark.sql(P_SQL_last_month)
    #last 3 month
    P_SQL_last_three_month = "SELECT "
    for i in groupby:
        P_SQL_last_three_month = P_SQL_last_three_month+i+","
    for i in Column_df:
        P_SQL_last_three_month = P_SQL_last_three_month+"max("+i+") as max_"+i+"_last_three_month,"
        P_SQL_last_three_month = P_SQL_last_three_month+"min("+i+") as min_"+i+"_last_three_month,"
        P_SQL_last_three_month = P_SQL_last_three_month+"avg("+i+") as avg_"+i+"_last_three_month,"
        P_SQL_last_three_month = P_SQL_last_three_month+"std("+i+") as avg_"+i+"_last_three_month,"
        
    P_SQL_last_three_month = P_SQL_last_three_month[:-1] +" from input_last_three_month "
    P_SQL_last_three_month = P_SQL_last_three_month + "group by "
    for i in groupby:
        P_SQL_last_three_month = P_SQL_last_three_month+i+","
    P_SQL_last_three_month = P_SQL_last_three_month[:-1]
    output_last_three_month = spark.sql(P_SQL_last_three_month)
    #join
    df_return = P_SQL_last_month.join(output_last_three_month,on=[groupby],how="inner")
    return df_return


# def l4_digital_mobile_web_agg_monthly_rolling_windows(mobile_web_agg_monthly: DataFrame) -> DataFrame:
#     if check_empty_dfs([mobile_web_agg_monthly]):
#         return get_spark_empty_df()

#     # previous month
#     Column_df = ["total_visit_count", "total_visit_duration", "total_volume_byte","total_download_byte","total_upload_byte"]

#     mobile_app_last_month = mobile_web_agg_monthly.filter(f.date_trunc("month", f.col("start_of_month")) == f.date_trunc("month", f.add_months(f.current_date(),-1)))
#     mobile_app_last_3_month = mobile_web_agg_monthly.filter(f.date_trunc("month", f.col("start_of_month")) == f.date_trunc("month", f.add_months(f.current_date(),-3)))

#     for i in Column_df:
#         mobile_app_last_month = mobile_app_last_month.withColumnRenamed(Column_df[i], Column_df[i] + "_last_month")

#     for i in Column_df3 :

#     # df_return = mobile_app_last_month,mobile_app_last_3_month
#     return df_return

