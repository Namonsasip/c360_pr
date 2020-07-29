from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.spark_util import get_spark_session


def l4_complaints_nps(
        input_df
):
    """
    Little hack to rename the column for NPS L4 features
    :param input_df:
    :return:
    """
    spark = get_spark_session()

    input_df.createOrReplaceTempView("input_table")

    df = spark.sql("""
        with intermediate_table as (
            select 
                subscription_identifier,
                start_of_week,
                sum(complaints_avg_nps*record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 604800 preceding and 1 preceding
                            ) as total_nps_last_week,
                sum(record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 604800 preceding and 1 preceding
                            ) as count_nps_last_week,
                            
                sum(complaints_avg_nps*record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 1209600 preceding and 1 preceding
                            ) as total_nps_last_two_week,
                sum(record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 1209600 preceding and 1 preceding
                            ) as count_nps_last_two_week,
                            
                sum(complaints_avg_nps*record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 2419200 preceding and 1 preceding
                            ) as total_nps_last_four_week,
                sum(record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 2419200 preceding and 1 preceding
                            ) as count_nps_last_four_week,
                            
                sum(complaints_avg_nps*record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 7257600 preceding and 1 preceding
                            ) as total_nps_last_twelve_week,
                sum(record_count) over (
                            partition by subscription_identifier 
                            order by cast(cast(start_of_week as timestamp) as long) asc
                            range between 7257600 preceding and 1 preceding
                            ) as count_nps_last_twelve_week
            from input_table
        )
        select
            subscription_identifier,
            start_of_week,
            total_nps_last_week/count_nps_last_week as complaints_avg_nps_last_week,
            total_nps_last_two_week/count_nps_last_two_week as complaints_avg_nps_last_two_week,
            total_nps_last_four_week/count_nps_last_four_week as complaints_avg_nps_last_four_week,
            total_nps_last_twelve_week/count_nps_last_week as complaints_avg_nps_last_twelve_week
        from intermediate_table
    """)

    return df
