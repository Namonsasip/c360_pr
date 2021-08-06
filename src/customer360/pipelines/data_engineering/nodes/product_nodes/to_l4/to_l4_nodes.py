from pyspark.sql import DataFrame

from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, union_dataframes_with_missing_cols,\
    gen_max_sql, execute_sql
from customer360.utilities.config_parser import l4_rolling_window


def rolling_window_product(data_frame: DataFrame,
                           param_1: dict,
                           param_2: dict) -> DataFrame:
    """
    :param data_frame:
    :param param_1:
    :param param_2:
    :return:
    """
    if check_empty_dfs([data_frame]):
        return get_spark_empty_df()

    group_cols = ["subscription_identifier", "start_of_week"]
    data_frame = data_frame.cache()
    param_1_df = l4_rolling_window(data_frame, param_1)
    param_2_df = l4_rolling_window(data_frame, param_2)

    union_df = union_dataframes_with_missing_cols(param_1_df, param_2_df)

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df


def add_l4_product_ratio_features(
        int_l4_product_df
) -> DataFrame:
    spark = get_spark_session()

    if len(int_l4_product_df.head(1)) == 0:
        return get_spark_empty_df()

    int_l4_product_df.createOrReplaceTempView("int_l4_product_df")

    result_df = spark.sql("""
        select
            *,
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_week as product_ratio_of_deactivated_one_off_to_rolling_data_ontop_last_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_week as product_ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_week as product_ratio_of_activated_one_off_to_rolling_data_ontop_last_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_week as product_ratio_of_activated_one_off_to_rolling_voice_ontop_last_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_two_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_two_week as product_ratio_of_deactivated_one_off_to_rolling_data_ontop_last_two_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_two_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_two_week as product_ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_two_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_two_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_two_week as product_ratio_of_activated_one_off_to_rolling_data_ontop_last_two_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_two_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_two_week as product_ratio_of_activated_one_off_to_rolling_voice_ontop_last_two_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_four_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_four_week as product_ratio_of_deactivated_one_off_to_rolling_data_ontop_last_four_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_four_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_four_week as product_ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_four_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_four_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_four_week as product_ratio_of_activated_one_off_to_rolling_data_ontop_last_four_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_four_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_four_week as product_ratio_of_activated_one_off_to_rolling_voice_ontop_last_four_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_twelve_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_twelve_week as product_ratio_of_deactivated_one_off_to_rolling_data_ontop_last_twelve_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_twelve_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_twelve_week as product_ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_twelve_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_twelve_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_twelve_week as product_ratio_of_activated_one_off_to_rolling_data_ontop_last_twelve_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_twelve_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_twelve_week as product_ratio_of_activated_one_off_to_rolling_voice_ontop_last_twelve_week
        from int_l4_product_df
    """)

    return result_df


def get_product_package_promotion_group_tariff_features(source_df: DataFrame) -> DataFrame:
    """
    :param source_df:
    :return:
    """
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    return source_df
