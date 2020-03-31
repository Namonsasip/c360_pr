import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os


# def l3_number_of_bs_used(input_df):
#     df = input_df.select('imsi', 'cell_id', 'time_in')
#     df = df.withColumn("start_of_month", f.to_date(f.date_trunc('month', "time_in"))) \
#         .drop(df.time_in)
#     return df
