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
    l5_calling_melody_prediction_score.count()
    l5_calling_melody_prediction_score = l5_calling_melody_prediction_score.dropDuplicates("subscription_identifier")
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
    # join prediction score and profile to the control group
    l5_calling_melody_prediction_score = l5_calling_melody_prediction_score.join(
        l0_calling_melody_control_group, ["old_subscription_identifier"], "inner"
    )
    # left join with existing user flag
    l5_calling_melody_prediction_score = l5_calling_melody_prediction_score.join(
        calling_melody_active_user, ["access_method_num"], "left"
    )

    #########################################
    # existing user campaign
    #
    existing_calling_melody_user = l5_calling_melody_prediction_score.where(
        "calling_melody_user = 1"
    )
    # Model based
    model_based_existing_calling_melody_user = existing_calling_melody_user.where(
        "group_name = 'TG'"
    )

    # Select top 3 deciles propensity score
    window = Window.partitionBy(F.col("model_name")).orderBy(F.col("propensity").desc())
    model_based_existing_calling_melody_user_target = model_based_existing_calling_melody_user.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(
        F.col("rank") <= 0.3
    )

    # Random select campaign invite message & offer
    (
        model_based_existing_calling_melody_user_target_CallingML_2_1,
        model_based_existing_calling_melody_user_target_CallingML_2_2,
        model_based_existing_calling_melody_user_target_CallingML_2_3,
        model_based_existing_calling_melody_user_target_CallingML_2_4,
        model_based_existing_calling_melody_user_target_CallingML_2_5,
        model_based_existing_calling_melody_user_target_CallingML_2_6,
        model_based_existing_calling_melody_user_target_CallingML_2_7,
    ) = model_based_existing_calling_melody_user_target.randomSplit(
        [0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428]
    )

    model_based_existing_calling_melody_user_target_CallingML_2_1 = model_based_existing_calling_melody_user_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.1"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_2 = model_based_existing_calling_melody_user_target_CallingML_2_2.withColumn(
        "campaign_child_code", "CallingML.2.2"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_3 = model_based_existing_calling_melody_user_target_CallingML_2_3.withColumn(
        "campaign_child_code", "CallingML.2.3"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_4 = model_based_existing_calling_melody_user_target_CallingML_2_4.withColumn(
        "campaign_child_code", "CallingML.2.4"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_5 = model_based_existing_calling_melody_user_target_CallingML_2_5.withColumn(
        "campaign_child_code", "CallingML.2.5"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_6 = model_based_existing_calling_melody_user_target_CallingML_2_6.withColumn(
        "campaign_child_code", "CallingML.2.6"
    )
    model_based_existing_calling_melody_user_target_CallingML_2_7 = model_based_existing_calling_melody_user_target_CallingML_2_7.withColumn(
        "campaign_child_code", "CallingML.2.7"
    )

    model_based_existing_calling_melody_user_target = (
        model_based_existing_calling_melody_user_target_CallingML_2_1.union(
            model_based_existing_calling_melody_user_target_CallingML_2_2
        )
        .union(model_based_existing_calling_melody_user_target_CallingML_2_3)
        .union(model_based_existing_calling_melody_user_target_CallingML_2_4)
        .union(model_based_existing_calling_melody_user_target_CallingML_2_5)
        .union(model_based_existing_calling_melody_user_target_CallingML_2_6)
        .union(model_based_existing_calling_melody_user_target_CallingML_2_7)
    )

    # Non-model
    non_model_existing_calling_melody_user = existing_calling_melody_user.where(
        "group_name = 'CG'"
    )
    # Random select campaign invite message & offer
    (
        non_model_existing_target,
        non_model_existing_not_target,
    ) = non_model_existing_calling_melody_user.randomSplit([0.3, 0.7])
    (
        non_model_existing_target_CallingML_2_1,
        non_model_existing_target_CallingML_2_2,
        non_model_existing_target_CallingML_2_3,
        non_model_existing_target_CallingML_2_4,
        non_model_existing_target_CallingML_2_5,
        non_model_existing_target_CallingML_2_6,
        non_model_existing_target_CallingML_2_7,
    ) = non_model_existing_target.randomSplit(
        [0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428]
    )
    non_model_existing_target_CallingML_2_1 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.1"
    )
    non_model_existing_target_CallingML_2_2 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.2"
    )
    non_model_existing_target_CallingML_2_3 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.3"
    )
    non_model_existing_target_CallingML_2_4 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.4"
    )
    non_model_existing_target_CallingML_2_5 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.5"
    )
    non_model_existing_target_CallingML_2_6 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.6"
    )
    non_model_existing_target_CallingML_2_7 = non_model_existing_target_CallingML_2_1.withColumn(
        "campaign_child_code", "CallingML.2.7"
    )
    non_model_existing_random_target = (
        non_model_existing_target_CallingML_2_1.union(
            non_model_existing_target_CallingML_2_2
        )
        .union(non_model_existing_target_CallingML_2_3)
        .union(non_model_existing_target_CallingML_2_4)
        .union(non_model_existing_target_CallingML_2_5)
        .union(non_model_existing_target_CallingML_2_6)
        .union(non_model_existing_target_CallingML_2_7)
    )
    #########################################
    # non user campaign
    #
    non_calling_melody_user = l5_calling_melody_prediction_score.where(
        "calling_melody_user != 1"
    )
    # Model based
    model_based_non_calling_melody_user = non_calling_melody_user.where(
        "group_name = 'TG'"
    )
    # Select top 3 deciles propensity score

    window = Window.partitionBy(F.col("model_name")).orderBy(F.col("propensity").desc())
    model_based_non_calling_melody_user_target = model_based_non_calling_melody_user.select(
        "*", F.percent_rank().over(window).alias("rank")
    ).filter(
        F.col("rank") <= 0.3
    )

    # Random select campaign invite message & offer
    (
        model_based_non_calling_melody_user_target_CallingML_1_1,
        model_based_non_calling_melody_user_target_CallingML_1_2,
        model_based_non_calling_melody_user_target_CallingML_1_3,
        model_based_non_calling_melody_user_target_CallingML_1_4,
        model_based_non_calling_melody_user_target_CallingML_1_5,
        model_based_non_calling_melody_user_target_CallingML_1_6,
        model_based_non_calling_melody_user_target_CallingML_1_7,
    ) = model_based_non_calling_melody_user_target.randomSplit(
        [0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428]
    )

    model_based_non_calling_melody_user_target_CallingML_1_1 = model_based_non_calling_melody_user_target_CallingML_1_1.withColumn(
        "campaign_child_code", "CallingML.1.1"
    )
    model_based_non_calling_melody_user_target_CallingML_1_2 = model_based_non_calling_melody_user_target_CallingML_1_2.withColumn(
        "campaign_child_code", "CallingML.1.2"
    )
    model_based_non_calling_melody_user_target_CallingML_1_3 = model_based_non_calling_melody_user_target_CallingML_1_3.withColumn(
        "campaign_child_code", "CallingML.1.3"
    )
    model_based_non_calling_melody_user_target_CallingML_1_4 = model_based_non_calling_melody_user_target_CallingML_1_4.withColumn(
        "campaign_child_code", "CallingML.1.4"
    )
    model_based_non_calling_melody_user_target_CallingML_1_5 = model_based_non_calling_melody_user_target_CallingML_1_5.withColumn(
        "campaign_child_code", "CallingML.1.5"
    )
    model_based_non_calling_melody_user_target_CallingML_1_6 = model_based_non_calling_melody_user_target_CallingML_1_6.withColumn(
        "campaign_child_code", "CallingML.1.6"
    )
    model_based_non_calling_melody_user_target_CallingML_1_7 = model_based_non_calling_melody_user_target_CallingML_1_7.withColumn(
        "campaign_child_code", "CallingML.1.7"
    )

    model_based_non_calling_melody_user_target = (
        model_based_non_calling_melody_user_target_CallingML_1_1.union(
            model_based_non_calling_melody_user_target_CallingML_1_2
        )
        .union(model_based_non_calling_melody_user_target_CallingML_1_3)
        .union(model_based_non_calling_melody_user_target_CallingML_1_4)
        .union(model_based_non_calling_melody_user_target_CallingML_1_5)
        .union(model_based_non_calling_melody_user_target_CallingML_1_6)
        .union(model_based_non_calling_melody_user_target_CallingML_1_7)
    )

    # Non-model
    non_model_non_calling_melody_user = non_calling_melody_user.where(
        "group_name = 'CG'"
    )
    (
        non_model_non_calling_melody_user_target,
        non_model_non_calling_melody_user_not_target,
    ) = non_model_non_calling_melody_user.randomSplit([0.3, 0.7])

    # Random select campaign invite message & offer
    (
        non_model_non_calling_melody_user_target_CallingML_1_1,
        non_model_non_calling_melody_user_target_CallingML_1_2,
        non_model_non_calling_melody_user_target_CallingML_1_3,
        non_model_non_calling_melody_user_target_CallingML_1_4,
        non_model_non_calling_melody_user_target_CallingML_1_5,
        non_model_non_calling_melody_user_target_CallingML_1_6,
        non_model_non_calling_melody_user_target_CallingML_1_7,
    ) = non_model_non_calling_melody_user_target.randomSplit(
        [0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428, 0.1428]
    )

    non_model_non_calling_melody_user_target_CallingML_1_1 = non_model_non_calling_melody_user_target_CallingML_1_1.withColumn(
        "campaign_child_code", "CallingML.1.1"
    )
    non_model_non_calling_melody_user_target_CallingML_1_2 = non_model_non_calling_melody_user_target_CallingML_1_2.withColumn(
        "campaign_child_code", "CallingML.1.2"
    )
    non_model_non_calling_melody_user_target_CallingML_1_3 = non_model_non_calling_melody_user_target_CallingML_1_3.withColumn(
        "campaign_child_code", "CallingML.1.3"
    )
    non_model_non_calling_melody_user_target_CallingML_1_4 = non_model_non_calling_melody_user_target_CallingML_1_4.withColumn(
        "campaign_child_code", "CallingML.1.4"
    )
    non_model_non_calling_melody_user_target_CallingML_1_5 = non_model_non_calling_melody_user_target_CallingML_1_5.withColumn(
        "campaign_child_code", "CallingML.1.5"
    )
    non_model_non_calling_melody_user_target_CallingML_1_6 = non_model_non_calling_melody_user_target_CallingML_1_6.withColumn(
        "campaign_child_code", "CallingML.1.6"
    )
    non_model_non_calling_melody_user_target_CallingML_1_7 = non_model_non_calling_melody_user_target_CallingML_1_7.withColumn(
        "campaign_child_code", "CallingML.1.7"
    )

    non_model_non_calling_melody_user_target = (
        non_model_non_calling_melody_user_target_CallingML_1_1.union(
            non_model_non_calling_melody_user_target_CallingML_1_2
        )
        .union(non_model_non_calling_melody_user_target_CallingML_1_3)
        .union(non_model_non_calling_melody_user_target_CallingML_1_4)
        .union(non_model_non_calling_melody_user_target_CallingML_1_5)
        .union(non_model_non_calling_melody_user_target_CallingML_1_6)
        .union(non_model_non_calling_melody_user_target_CallingML_1_7)
    )

    all_targeted_calling_melody = (
        model_based_existing_calling_melody_user_target.union(
            non_model_existing_random_target
        )
        .union(model_based_non_calling_melody_user_target)
        .union(non_model_non_calling_melody_user_target)
    )
    all_targeted_calling_melody = all_targeted_calling_melody.selectExpr(
        "*", "date('2020-01-11') as target_list_date"
    )
    all_targeted_calling_melody.createOrReplaceTempView("tmp_load_view")
    spark.sql(
        """CREATE TABLE prod_musicupsell.calling_melody_target_history"
                 USING DELTA
                 AS
                 SELECT * FROM tmp_load_view"""
    )
    all_targeted_calling_melody_pdf = all_targeted_calling_melody.selectExpr(
        "date('2020-01-12') as contact_date",
        "old_subscription_identifier",
        "campaign_child_code",
    ).toPandas()

    list_date = datetime.datetime.now() + datetime.timedelta(hours=7)
    all_targeted_calling_melody_pdf.to_csv(
        "/dbfs/mnt/customer360-blob-output/MUSIC/PCM/MUSIC_UPSELL_PCM_"
        + datetime.datetime.strptime(
            (list_date + datetime.timedelta(days=0)).strftime("%Y-%m-%d"), "%Y-%m-%d"
        ).strftime("%Y%m%d")
        + ".csv",
        index=False,
        sep="|",
        header=False,
        encoding="utf-8-sig",
    )
    return all_targeted_calling_melody
