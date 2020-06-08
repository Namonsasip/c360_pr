import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics
from customer360.utilities.spark_util import get_spark_session

def l3_geo_time_spent_by_location_monthly(df,sql):
    df=node_from_config(df,sql)
    return df

def l3_geo_area_from_ais_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

def l3_geo_area_from_competitor_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

