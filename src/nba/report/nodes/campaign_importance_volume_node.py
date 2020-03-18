from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import functions as F
from typing import Dict, Any
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session
print("yay")
spark = get_spark_session()

def create_table_delta(distinct_response:DataFrame) -> DataFrame:
    df_agg = distinct_response
    return df_agg

def read_table(df_agg:DataFrame):
    df_agg.show()
    return 1