from pyspark.sql import DataFrame

from src.customer360.utilities.spark_util import get_spark_session, get_spark_empty_df


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
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_week as ratio_of_deactivated_one_off_to_rolling_data_ontop_last_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_week as ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_week as ratio_of_activated_one_off_to_rolling_data_ontop_last_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_week as ratio_of_activated_one_off_to_rolling_voice_ontop_last_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_two_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_two_week as ratio_of_deactivated_one_off_to_rolling_data_ontop_last_two_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_two_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_two_week as ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_two_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_two_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_two_week as ratio_of_activated_one_off_to_rolling_data_ontop_last_two_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_two_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_two_week as ratio_of_activated_one_off_to_rolling_voice_ontop_last_two_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_four_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_four_week as ratio_of_deactivated_one_off_to_rolling_data_ontop_last_four_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_four_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_four_week as ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_four_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_four_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_four_week as ratio_of_activated_one_off_to_rolling_data_ontop_last_four_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_four_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_four_week as ratio_of_activated_one_off_to_rolling_voice_ontop_last_four_week,
            
            sum_product_deactivated_one_off_data_ontop_packages_weekly_last_twelve_week/sum_product_deactivated_rolling_data_ontop_packages_weekly_last_twelve_week as ratio_of_deactivated_one_off_to_rolling_data_ontop_last_twelve_week,
            sum_product_deactivated_one_off_voice_ontop_packages_weekly_last_twelve_week/sum_product_deactivated_rolling_voice_ontop_packages_weekly_last_twelve_week as ratio_of_deactivated_one_off_to_rolling_voice_ontop_last_twelve_week,
            sum_product_activated_one_off_data_ontop_packages_weekly_last_twelve_week/sum_product_activated_rolling_data_ontop_packages_weekly_last_twelve_week as ratio_of_activated_one_off_to_rolling_data_ontop_last_twelve_week,
            sum_product_activated_one_off_voice_ontop_packages_weekly_last_twelve_week/sum_product_activated_rolling_voice_ontop_packages_weekly_last_twelve_week as ratio_of_activated_one_off_to_rolling_voice_ontop_last_twelve_week
        from int_l4_product_df
    """)

    return result_df
