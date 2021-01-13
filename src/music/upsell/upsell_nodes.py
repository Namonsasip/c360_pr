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


def create_calling_melody_upsell(
    l0_product_ru_a_callingmelody_daily,
    l0_calling_melody_control_group,
    l5_calling_melody_prediction_score,
):
    l0_product_ru_a_callingmelody_daily = catalog.load(
        "l0_product_ru_a_callingmelody_daily"
    )
    l0_calling_melody_control_group = catalog.load("l0_calling_melody_control_group")
    l5_calling_melody_prediction_score = catalog.load(
        "l5_calling_melody_prediction_score"
    )
    # calling melody transactions are use to identify who is an existing user
    l0_product_ru_a_callingmelody_daily = l0_product_ru_a_callingmelody_daily.selectExpr(
        "DATE(CONCAT(YEAR(date(day_id)),'-',MONTH(date(day_id)),'-01')) as start_of_month",
        "date(day_id) as day_id",
        "access_method_num",
        "rbt_sub_group",
    )
    l0_product_ru_a_callingmelody_daily = (
        l0_product_ru_a_callingmelody_daily.groupby("day_id", "access_method_num")
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
        .withColumn("calling_melody_user", F.lit(1))
    )
    max_date = (
        l0_product_ru_a_callingmelody_daily.groupby("calling_melody_user")
        .agg(F.max("day_id").alias("day_id"))
        .collect()
    )
    max_date[0][1]
    # if user has transaction within the pass 30 days we consider user as an existing user
    calling_melody_active_user = (
        l0_product_ru_a_callingmelody_daily.where(
            "day_id >= date_sub(DATE('" + str(max_date[0][1]) + "'),30)"
        )
        .groupby("access_method_num")
        .agg(F.max("calling_melody_user").alias("calling_melody_user"))
    )
    # take only cg and tg into account
    l0_calling_melody_control_group = l0_calling_melody_control_group.where(
        "group_name in ('TG','CG')"
    )
    # join prediction score with profile
    l5_calling_melody_prediction_score = l5_calling_melody_prediction_score.join(
        l0_calling_melody_control_group, ["old_subscription_identifier"], "inner"
    )

    l5_calling_melody_prediction_score = l5_calling_melody_prediction_score.join(
        calling_melody_active_user, ["access_method_num"], "left"
    )
