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

mapping_for_model_training = catalog.load("mapping_for_model_training")
dm996_cvm_ontop_pack = catalog.load("dm996_cvm_ontop_pack")