from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import functions as F
from typing import Dict, Any
from pyspark.sql import DataFrame
import pandas as pd

print("yay")
df = catalog.load("l1_prep_report_ontop_pack")
df.show()
def create_table_delta(path:str, table_name:str):
    df = spark.sql("select * from prod_delta.dm01_fin_top_up")
    return 0