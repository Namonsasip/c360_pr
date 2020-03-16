from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import functions as F
import pandas as pd

today = date.today()
today.strftime("%Y-%m-%d %H:%M:%S")
DATA_PACK_SOURCE = "dm42_promotion_prepaid"
DATA_PACK_DATE_COL = "date_id"
VOICE_PACK_SOURCE = "dm43_promotion_prepaid"
VOICE_PACK_DATE_COL = "date_id"
CVM_PREPAID_GROUPS = "cvm_prepaid_customer_groups"
CAMPAIGN_MAPPING = "use_case_campaign_mapping"
CAMPAIGN_SOURCE = "dm996_cvm_ontop_pack"
CAMPAIGN_DATE_COL = "ddate"


def get_dataframe_max_date(date_col, sdf: F.DataFrame):
    """
    This function will retrieve maximum date in specify column of spark dataframe
    Args:
     date_col: datetime column name
     sdf: spark dataframe

    Returns: max date within dataframe in datetime type

    """
    delta_day = timedelta(days=60)
    scoped_date = today - delta_day
    sdf_filter = sdf.filter(
        F.col(date_col) > F.unix_timestamp(F.lit(scoped_date)).cast("timestamp")
    )
    max_date = (
        sdf_filter.withColumn("G", F.lit(1))
        .groupBy("G")
        .agg(F.max(date_col))
        .collect()[0]["max(" + date_col + ")"]
    )
    return max_date


def agg_groupby_key_over_period(
    groupby_identifiers, expr, sdf, date_col, start_period, end_period
):
    selected_period_sdf = sdf.filter(
        F.col(date_col).between(
            pd.to_datetime(start_period.strftime("%Y-%m-%d")),
            pd.to_datetime(end_period.strftime("%Y-%m-%d")),
        )
    )
    result = selected_period_sdf.groupBy(groupby_identifiers).agg(*expr)
    return result


datetime_str = "2020-03-03"
datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d")
day = datetime.date(datetime_object)
res = sum_groupby_key_over_period(
    ["analytic_id", "register_date"],
    "total_net_tariff",
    DATA_PACK_SOURCE,
    DATA_PACK_DATE_COL,
    day,
    day,
)
res.show()


def generate_usecase_view_report():
    data_ontop_sdf = catalog.load(DATA_PACK_SOURCE)
    voice_ontop_sdf = catalog.load(VOICE_PACK_SOURCE)
    campaign_tracking_sdf = catalog.load(CAMPAIGN_SOURCE)

    max_data_ontop_date = get_dataframe_max_date(DATA_PACK_DATE_COL, data_ontop_sdf)
    max_voice_ontop_date = get_dataframe_max_date(VOICE_PACK_DATE_COL, voice_ontop_sdf)
    max_campaign_tracking_date = get_dataframe_max_date(
        CAMPAIGN_DATE_COL, campaign_tracking_sdf
    )
    datetime_str = "2020-03-06"
    datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d")
    day = datetime.date(datetime_object)
    end_period = day
    if (
        end_period > max_data_ontop_date
        or end_period > max_voice_ontop_date
        or end_period > max_campaign_tracking_date
    ):
        raise Exception(
            "Data source delay"
        )  # Will provide more useful information later

    prepaid_groups_sdf = catalog.load(CVM_PREPAID_GROUPS)
    campaign_mapping_sdf = catalog.load(CAMPAIGN_MAPPING)

    delta_day = timedelta(days=90)
    scoped_date = end_period - delta_day
    campaign_tracking_sdf_filter = campaign_tracking_sdf.filter(
        F.col("contact_date") > F.unix_timestamp(F.lit(scoped_date)).cast("timestamp")
    )
    campaign_selected_columns = [
        "analytic_id",
        "register_date",
        "campaign_child_code",
        "contact_date",
        "response",
    ]
    cvm_campaign_tracking = prepaid_groups_sdf.join(
        campaign_tracking_sdf_filter.select(campaign_selected_columns),
        ["analytic_id", "register_date"],
        "inner",
    ).join(campaign_mapping_sdf, ["campaign_child_code"], "inner")
    cvm_campaign_tracking.persist()

    current_size = (
        prepaid_groups_sdf.groupby("target_group")
        .agg(F.countDistinct("crm_sub_id").alias("distinct_targeted_subscriber"))
        .toPandas()
    )

    delta_day = timedelta(days=6)
    last_week = end_period - delta_day
    delta_day = timedelta(days=29)
    last_month = end_period - delta_day
    exprs = [F.count("*").alias("CNT")]
    report_group_by = ["target_group", "usecase", "response"]
    targeted_subscriber_ytd = agg_groupby_key_over_period(
        report_group_by,
        exprs,
        cvm_campaign_tracking,
        "contact_date",
        end_period,
        end_period,
    ).toPandas()
    targeted_subscriber_week = agg_groupby_key_over_period(
        report_group_by,
        exprs,
        cvm_campaign_tracking,
        "contact_date",
        last_week,
        end_period,
    ).toPandas()
    targeted_subscriber_month = agg_groupby_key_over_period(
        report_group_by,
        exprs,
        cvm_campaign_tracking,
        "contact_date",
        last_month,
        end_period,
    ).toPandas()

    sent_and_accept = (
        cvm_campaign_tracking.filter(
            F.col("contact_date")
            == F.unix_timestamp(F.lit(end_period)).cast("timestamp")
        )
        .groupby("target_group", "usecase", "response")
        .agg(F.count("*").alias("CNT"))
        .toPandas()
    )

    return cvm_campaign_tracking


df = generate_usecase_view_report()
df.show()

exprs = [F.count("*").alias("CNT")]
df.groupby("target_group", "usecase", "response").agg(*exprs).show()


df.groupby("target_group", "usecase", "response").agg(
    F.count("*"), F.countDistinct("crm_sub_id")
).show()
