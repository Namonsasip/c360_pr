from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion
from kedro.context.context import load_context
from pathlib import Path
import logging, os


def build_campaign_l2_layer(l1_campaign_post_pre_daily: DataFrame,
                            l1_campaign_top_channel_daily: DataFrame) -> DataFrame:
    """

    :param l1_campaign_post_pre_daily:
    :param l1_campaign_top_channel_daily:
    :return:
    """
