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
def customer_app_category_windows (mobile_app: DataFrame) -> DataFrame :

    if check_empty_dfs([mobile_app]):
        return get_spark_empty_df()

    #filter month
    Column_df = ["total_visit_count","total_visit_duration","total_volume_byte"]
    mobile_app_last_month =  mobile_app.filter(f.date_trunc("month", f.col("start_of_month")) == f.date_trunc("month", f.add_months(f.current_date(), -1))).limit(1)
    mobile_app_last_3_month =  mobile_app.filter(f.date_trunc("month", f.col("start_of_month")) == f.date_trunc("month", f.add_months(f.current_date(), -3))).limit(1)
    
    for i in Column_df:
        mobile_app_last_month = mobile_app_last_month.withColumnRenamed(Column_df[i] ,Column_df[i]+"_last_month")

    df_return = mobile_app_last_month
    return df_return