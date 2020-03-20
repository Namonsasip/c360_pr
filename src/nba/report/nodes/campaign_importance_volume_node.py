from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import functions as F
from typing import Dict, Any
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session
print("yay")
spark = get_spark_session()


def create_l0_campaign_history_master_active(input_campaign_master:DataFrame) -> DataFrame:
    return input_campaign_master

def read_table(campaign_master2:DataFrame):
    campaign_master2.limit(1).show()
