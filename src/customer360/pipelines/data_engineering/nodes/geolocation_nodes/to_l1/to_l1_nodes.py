import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics
from pyspark.sql import Window

from customer360.utilities.re_usable_functions import add_start_of_week_and_month, union_dataframes_with_missing_cols, \
    execute_sql


