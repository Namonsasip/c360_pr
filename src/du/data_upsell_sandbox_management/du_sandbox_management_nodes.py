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
import logging


def update_du_control_group_nodes(
    unused_memory_update_groups, delta_table_schema, mode,
):
    spark = get_spark_session()
    sandbox_framework_2021 = spark.sql(
        "SELECT * FROM prod_dataupsell.sandbox_framework_2021"
    )
    # Usecase Control Group sampling rate and naming
    sampling_rate_bau = [0.975, 0.025]
    sampling_rate_reference = [0.975, 0.025]
    sampling_rate_new_experiment = [0.975, 0.025]

    control_group_names_in_bau = ["BAU_TG", "BAU_CG"]
    control_group_names_in_reference = ["REF_TG", "REF_CG"]
    control_group_names_in_new_experiment = ["EXP_TG", "EXP_CG"]
    du_control_group_exc_GCG_sub = spark.sql(
        """"SELECT * FROM prod_dataupsell.data_upsell_usecase_control_group_2021 
        WHERE usecase_control_group != 'GCG' AND register_date is not null"""
    )
    # ALL GCG
    test_groups = sandbox_framework_2021.where("sandbox_flag = 'gcg'").withColumn(
        "usecase_control_group", F.lit("GCG")
    )
    # exclude GCG
    cvm_sandbox_excl_gcg = sandbox_framework_2021.where("sandbox_flag != 'gcg'")
    new_sub_unassign = cvm_sandbox_excl_gcg.join(
        du_control_group_exc_GCG_sub,
        ["old_subscription_identifier", "register_date"],
        "left_anti",
    )
    logging.warning(f"Updating {new_sub_unassign.count()} new sub")

    # Sampling BAU control group
    group_list = new_sub_unassign.where("sandbox_flag = 'bau_2021'").randomSplit(
        sampling_rate_bau
    )
    iterate_n = 0
    for g in group_list:
        test_groups = test_groups.selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "mobile_status",
            "global_control_group",
            "sandbox_flag",
            "mckinsey_flag",
            "usecase_control_group",
        ).union(
            g.selectExpr(
                "old_subscription_identifier",
                "date(register_date) as register_date",
                "mobile_status",
                "global_control_group",
                "sandbox_flag",
                "mckinsey_flag",
                "'"
                + control_group_names_in_bau[iterate_n]
                + "' as usecase_control_group",
            )
        )
        iterate_n += 1

    # Sampling reference control group
    group_list = new_sub_unassign.where("sandbox_flag = 'bau_2020'").randomSplit(
        sampling_rate_reference
    )
    iterate_n = 0
    for g in group_list:
        test_groups = test_groups.selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "mobile_status",
            "global_control_group",
            "sandbox_flag",
            "mckinsey_flag",
            "usecase_control_group",
        ).union(
            g.selectExpr(
                "old_subscription_identifier",
                "date(register_date) as register_date",
                "mobile_status",
                "global_control_group",
                "sandbox_flag",
                "mckinsey_flag",
                "'"
                + control_group_names_in_reference[iterate_n]
                + "' as usecase_control_group",
            )
        )
        iterate_n += 1

    # Sampling New Experiment control group
    group_list = new_sub_unassign.where("sandbox_flag = 'new_experiment'").randomSplit(
        sampling_rate_new_experiment
    )
    iterate_n = 0
    for g in group_list:
        test_groups = test_groups.selectExpr(
            "old_subscription_identifier",
            "date(register_date) as register_date",
            "mobile_status",
            "global_control_group",
            "sandbox_flag",
            "mckinsey_flag",
            "usecase_control_group",
        ).union(
            g.selectExpr(
                "old_subscription_identifier",
                "date(register_date) as register_date",
                "mobile_status",
                "global_control_group",
                "sandbox_flag",
                "mckinsey_flag",
                "'"
                + control_group_names_in_new_experiment[iterate_n]
                + "' as usecase_control_group",
            )
        )
        iterate_n += 1

    updated_du_control_group_status = (
        du_control_group_exc_GCG_sub.select(
            "old_subscription_identifier",
            "register_date",
            "sandbox_flag",
            "usecase_control_group",
        )
        .join(
            sandbox_framework_2021.select(
                "old_subscription_identifier",
                "register_date",
                "mobile_status",
                "global_control_group",
                "mckinsey_flag",
            ),
            ["old_subscription_identifier", "register_date"],
            "inner",
        )
        .select(
            "old_subscription_identifier",
            "register_date",
            "mobile_status",
            "global_control_group",
            "sandbox_flag",
            "mckinsey_flag",
            "usecase_control_group",
        )
    )
    test_groups = test_groups.union(updated_du_control_group_status)
    test_groups = test_groups.dropDuplicates(
        ["old_subscription_identifier", "register_date"]
    )
    test_groups.createOrReplaceTempView("usecase_control_group")
    logging.warning(f"Deleting Tmp table")
    spark.sql(
        "DROP TABLE IF EXISTS "
        + delta_table_schema
        + ".data_upsell_usecase_control_group_2021_tmp"
    )
    logging.warning(f"Save updated {mode} control group to Tmp table")
    spark.sql(
        "CREATE TABLE "
        + delta_table_schema
        + ".data_upsell_usecase_control_group_2021_tmp AS SELECT * FROM usecase_control_group"
    )
    logging.warning(f"Deleting {mode} control group")
    spark.sql(
        "DROP TABLE IF EXISTS "
        + delta_table_schema
        + ".data_upsell_usecase_control_group_2021"
    )
    logging.warning(f"Replace {mode} control group with updated tmp table")
    spark.sql(
        """CREATE TABLE """
        + delta_table_schema
        + """.data_upsell_usecase_control_group_2021 
        AS 
        SELECT * FROM """
        + delta_table_schema
        + """.data_upsell_usecase_control_group_2021_tmp"""
    )
    return test_groups
