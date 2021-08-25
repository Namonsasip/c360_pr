import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, List, Tuple, Union

import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from customer360.utilities.spark_util import get_spark_session
import logging


def update_disney_cg_tg_group(delta_table_schema: str,
                              dummy):
    """
    This function generates the Disney+ target based on BAU conditions inner join with the CG and TG for all
    REF, BAU, and NEW_EXP that is generated daily from the data upsell pipeline.

    Finally, save the table at disney_usecase_control_group_table

    """
    spark = get_spark_session()

    ##################
    # Active customer #
    ##################
    customer_profile = spark.sql(
        "SELECT * FROM C360_external.l3_customer_profile_include_1mo_non_active WHERE subscription_status='SA' AND "
        "charge_type='Pre-paid' AND subscription_identifier IS NOT NULL")

    customer_profile_latest_active_prepaid = customer_profile.orderBy('partition_month',
                                                                      ascending=False).dropDuplicates(
        ['access_method_num', 'old_subscription_identifier', 'subscription_identifier', 'register_date'])

    ##################
    # Exclude NINO user #
    ##################
    no_activity = spark.sql("SELECT * FROM c360_l0.usage_ru_f_no_activity_daily")
    latest_partition_date = no_activity.groupby(
        "c360_subscription_identifier", "access_method_num"
    ).agg(F.max("partition_date").alias("partition_date"))

    no_activity_latest_date = no_activity.join(
        latest_partition_date,
        [
            "c360_subscription_identifier",
            "access_method_num",
            "partition_date"
        ],
        "inner",
    )
    no_activity_latest_date_nino = no_activity_latest_date.filter(no_activity_latest_date['no_activity_n_days'] <= 90)

    ##################
    ## Smartphone ##
    ##################

    handset_prepaid_active_smartphone = spark.sql(
        "SELECT * FROM c360_l0.device_summary_customer_handset_daily WHERE network_type = '3GPre-paid' AND "
        "mobile_status = 'SA' AND handset_type = 'SmartPhone'")

    handset_prepaid_active_smartphone_latest_date = handset_prepaid_active_smartphone.orderBy('partition_date',
                                                                                              ascending=False).dropDuplicates(
        ['mobile_no'])

    ##################
    ## Disney+ purchaser ##
    ##################
    disney_purchaser = spark.sql(
        "SELECT c360_subscription_identifier as subscription_identifier, access_method_num, date(day_id) as "
        "contact_date FROM c360_l0.product_ru_a_vas_package_daily WHERE package_id in ('7400313', '7400314', "
        "'7400337', '7400331', '7400312', '7400340', '7400341', '7400329', '7400345', '7400330', '7400315', "
        "'7400336') AND date(day_id) > '2021-06-01'")

    ##################
    #### Persona #####
    ##################
    persona = spark.sql("SELECT * FROM prod_persona.digital_persona_prepaid_monthly_production")
    cond = (persona['digital_cluster_name'] != 'non_digital') & (persona['video_streaming_scoring'] >= 1.5)
    persona_g1_g5_streaming_more_than_1_5 = persona.filter(cond)

    persona_g1_g5_streaming_more_than_1_5_sorted = persona_g1_g5_streaming_more_than_1_5.orderBy('month_id',
                                                                                                 ascending=False)
    persona_g1_g5_streaming_more_than_1_5_sorted = persona_g1_g5_streaming_more_than_1_5_sorted.dropDuplicates(
        ['crm_sub_id'])

    ##################
    #### COMBINE ####
    ##################
    """
    Procedures

    1. Inner join ACTIVE USER with PERSONA (with old_sub_id)
    2. Inner join with NOT NINO user (with sub_id and mobile_no)
    3. Inner join with SMARTPHONE USER (with mobile_no)
    4. Left anti join (exclude) with DISNEY+ PURCHASER (with sub_id and mobile_no)
    """

    disney_bau_list = \
        customer_profile_latest_active_prepaid.join(
            persona_g1_g5_streaming_more_than_1_5_sorted.withColumnRenamed('crm_sub_id'
                                                                           , 'old_subscription_identifier'),
            on=['old_subscription_identifier'], how='inner'
        ).join(no_activity_latest_date_nino.select('c360_subscription_identifier'
                                                   , 'access_method_num', 'no_activity_n_days'
                                                   ).withColumnRenamed('c360_subscription_identifier',
                                                                       'subscription_identifier'),
               on=['subscription_identifier'
                   , 'access_method_num'], how='inner'
               ).join(handset_prepaid_active_smartphone_latest_date.select('mobile_no'
                                                                           , 'handset_type').withColumnRenamed(
            'mobile_no',
            'access_method_num'), on=['access_method_num'],
            how='inner').join(disney_purchaser,
                              on=['subscription_identifier', 'access_method_num'],
                              how='left_anti')

    ######################################################
    ######## Get CG & TG from the data upsell ########
    ######################################################
    data_upsell_usecase_control_group_2021 = spark.sql(
        'SELECT * FROM prod_dataupsell.data_upsell_usecase_control_group_2021')

    du_cg_tg = data_upsell_usecase_control_group_2021.where(
        "usecase_control_group != 'GCG'").select('old_subscription_identifier',
                                                 'usecase_control_group',
                                                 'global_control_group')

    disney_cg_tg_exclude_gcg = disney_bau_list.join(du_cg_tg,
                                                    on=['old_subscription_identifier'],
                                                    how='inner').select('old_subscription_identifier',
                                                                        'subscription_identifier',
                                                                        'access_method_num', 'usecase_control_group',
                                                                        'global_control_group')

    disney_cg_tg_exclude_gcg.write.format("delta").mode("overwrite").saveAsTable(
        delta_table_schema + ".disney_usecase_control_group_table")
