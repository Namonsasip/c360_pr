from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_start_of_week_and_month, add_event_week_and_month_from_yyyymmdd
from pyspark.sql import functions as f, DataFrame
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from pyspark.sql.types import *


def l1_complaints_shop_training(input, input_cust):
    spark = get_spark_session()
    # spark.udf.register("getSurveyScoreNumber", getSurveyScoreNumber)
    input.registerTempTable("complaints_acc_qmt_csi")

    stmt = """select partition_date
            ,access_method_num as access_method_num
            ,round(avg(case when survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก') and location_shop_name_en not like 'Serenade%' then getSurveyScoreNumber(survey_result) else null end)) as complaints_avg_csi_shop_score
            ,round(avg(case when survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก') and location_shop_name_en like 'Serenade%' then getSurveyScoreNumber(survey_result) else null end)) as complaints_avg_csi_serenade_club_score
            ,round(avg(case when survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10') and location_shop_name_en not like 'Serenade%' then survey_nps_score else null end)) as complaints_avg_nps_shop_score
            ,round(avg(case when survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10') and location_shop_name_en like 'Serenade%' then survey_nps_score else null end)) as complaints_avg_nps_serenade_club_score
            from complaints_acc_qmt_csi
            where (survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก')
                  or survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10')
                )
            and access_method_num is not null
            group by partition_date,access_method_num"""
    stmt_full = """select partition_date,access_method_num
                    ,round(avg(case when complaints_csi_shop_score in ('Very Dissatisfied','ไม่พอใจมาก') then '1'
                    when complaints_csi_shop_score in ('Dissatisfied','ไม่พอใจ') then '2'
                    when complaints_csi_shop_score in ('Neutral','ปานกลาง') then '3'
                    when complaints_csi_shop_score in ('Satisfied','พอใจ') then '4'
                    when complaints_csi_shop_score in ('Very Satisfied','พอใจมาก') then '5'
                    else null end)) as complaints_avg_csi_shop_score
                    ,round(avg(case when complaints_csi_serenade_club_score in ('Very Dissatisfied','ไม่พอใจมาก') then '1'
                    when complaints_csi_serenade_club_score in ('Dissatisfied','ไม่พอใจ') then '2'
                    when complaints_csi_serenade_club_score in ('Neutral','ปานกลาง') then '3'
                    when complaints_csi_serenade_club_score in ('Satisfied','พอใจ') then '4'
                    when complaints_csi_serenade_club_score in ('Very Satisfied','พอใจมาก') then '5'
                    else null end)) as complaints_avg_csi_serenade_club_score
                    ,round(avg(complaints_nps_shop_score)) as complaints_avg_nps_shop_score
                    ,round(avg(complaints_nps_serenade_club_score)) as complaints_avg_nps_serenade_club_score
                    from
                    (
                    select partition_date
                    ,access_method_num
                    ,case when survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก') and location_shop_name_en not like 'Serenade%' then survey_result else null end as complaints_csi_shop_score
                    ,case when survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก') and location_shop_name_en like 'Serenade%' then survey_result else null end as complaints_csi_serenade_club_score
                    ,case when survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10') and location_shop_name_en not like 'Serenade%' then survey_nps_score else null end as complaints_nps_shop_score
                    ,case when survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10') and location_shop_name_en like 'Serenade%' then survey_nps_score else null end as complaints_nps_serenade_club_score
                    from complaints_acc_qmt_csi
                    where (survey_result in ('Very Dissatisfied','Dissatisfied','Neutral','Satisfied','Very Satisfied','ไม่พอใจมาก','ไม่พอใจ','ปานกลาง','พอใจ','พอใจมาก')
                    or survey_nps_score in ('0','1','2','3','4','5','6','7','8','9','10')
                    )
                    and access_method_num is not null
                    )
                    group by partition_date,access_method_num"""

    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    input_cust = input_cust.select('subscription_status', 'access_method_num', 'subscription_identifier', 'event_partition_date')
    # cond = [df.access_method_num == input_cust.access_method_num, df.event_partition_date == input_cust.event_partition_date]
    df_output = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'left')

    return df_output


def l1_complaints_ai_chat_survey_training(input, config):
    df = node_from_config(input, config)

    return df


def change_grouped_column_name(
        input_df,
        config
):
    df = node_from_config(input_df, config)
    for alias, col_name in config["rename_column"].items():
        df = df.withColumnRenamed(col_name, alias)

    return df


def dac_for_complaints_to_l1_pipeline(
        input_df: DataFrame,
        cust_df: DataFrame,
        target_table_name: str,
        exception_partiton_list=None):
    """
    :param input_df:
    :param cust_df:
    :param target_table_name:
    :param exception_partiton_list:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name,
                                                       exception_partitions = exception_partiton_list)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]
