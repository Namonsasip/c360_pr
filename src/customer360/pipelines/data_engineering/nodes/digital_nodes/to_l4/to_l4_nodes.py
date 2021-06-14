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

    
    return mobile_app