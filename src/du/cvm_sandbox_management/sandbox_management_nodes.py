import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, List, Tuple, Union

import pandas as pd
import plotnine
from plotnine import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from customer360.utilities.spark_util import get_spark_session

def update_sandbox_control_group(
    l0_du_pre_experiment3_groups,
    l0_customer_profile_profile_customer_profile_pre_current_full_load,
    sampling_rate,
    test_group_name,
    test_group_flag,
):
    return test_groups