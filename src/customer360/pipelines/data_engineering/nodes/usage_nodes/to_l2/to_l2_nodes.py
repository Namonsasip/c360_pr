from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F


def usage_merge_all_data(l2_usage_call_relation_sum_weekly: DataFrame,
                         l2_usage_call_relation_sum_ir_weekly: DataFrame,
                         l2_usage_data_prepaid_postpaid_weekly: DataFrame,
                         l2_usage_ru_a_vas_postpaid_prepaid_weekly: DataFrame) -> DataFrame:
    """
    :param l2_usage_call_relation_sum_weekly:
    :param l2_usage_call_relation_sum_ir_weekly:
    :param l2_usage_data_prepaid_postpaid_weekly:
    :param l2_usage_ru_a_vas_postpaid_prepaid_weekly:
    :return:
    """
    join_key = ['crm_sub_id', 'start_of_week']
    final_df = l2_usage_call_relation_sum_weekly.join(l2_usage_call_relation_sum_ir_weekly, join_key, 'outer')
    final_df = final_df.join(l2_usage_data_prepaid_postpaid_weekly, join_key, 'outer')
    final_df = final_df.join(l2_usage_ru_a_vas_postpaid_prepaid_weekly, join_key, 'outer')

    return final_df
