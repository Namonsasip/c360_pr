import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, Any, List

import pandas as pd
import plotnine
from plotnine import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from customer360.utilities.spark_util import get_spark_session


def gcg_contamination_checking_report():
    l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
        "l0_campaign_tracking_contact_list_pre_full_load"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.where(
        "date(contact_date) > date('2020-08-01')"
    )

    return df
