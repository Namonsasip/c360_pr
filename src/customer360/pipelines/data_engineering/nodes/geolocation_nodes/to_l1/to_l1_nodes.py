from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging, os

from customer360.utilities.re_usable_functions import add_event_week_and_month_from_yyyymmdd, check_empty_dfs, \
    data_non_availability_and_missing_check, union_dataframes_with_missing_cols ,get_max_date_from_master_data
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)
log = logging.getLogger(__name__)

# def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
#     max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]
#     logging.info("Max date of master is [{0}]".format(max_date))
#     input_df = input_df.where('{0}='.format(par_col) + str(max_date))
#     return input_df


def distance_calculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) +
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) *
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast(DecimalType(13, 2))


def l1_geo_mst_location_near_shop_master(master_df: DataFrame, shape_df: DataFrame) -> DataFrame:
    if check_empty_dfs([master_df, shape_df]):
        return get_spark_empty_df()

    master_df = get_max_date_from_master_data(master_df, 'partition_date')
    shape_df = get_max_date_from_master_data(shape_df, 'partition_month')

    output_df = master_df.crossJoin(shape_df) \
        .select('landmark_sub_name_en', 'landmark_cat_name_en', 'location_id', 'location_name',
                distance_calculate_statement('landmark_latitude', 'landmark_longitude', 'latitude', 'longitude')
                .alias('distance_km')) \
        .where("landmark_cat_name_en in ('AIS', 'DTAC', 'TRUE') and distance_km <= (0.5)") \
        .dropDuplicates()

    return output_df


def l1_geo_mst_location_ais_shop_master(shape_df: DataFrame) -> DataFrame:
    if check_empty_dfs([shape_df]):
        return get_spark_empty_df()

    shape_df = get_max_date_from_master_data(shape_df, 'partition_month')

    # output_df = shape_df.filter("landmark_cat_name_en in ('AIS', 'DTAC', 'TRUE')")
    output_df = shape_df.filter("landmark_cat_name_en = 'AIS'")

    return output_df


def l1_geo_mst_cell_masterplan_master(master_df: DataFrame) -> DataFrame:
    if check_empty_dfs([master_df]):
        return get_spark_empty_df()
    return get_max_date_from_master_data(master_df, 'partition_date')


def l1_geo_time_spent_by_store_daily(timespent_df: DataFrame, master_df: DataFrame, param_config: str) -> DataFrame:
    output_df = timespent_df.join(master_df, ['location_id'], 'inner').select(
        'imsi', timespent_df.location_id,
        'duration',
        'num_visit', 'distance_km',
        'landmark_sub_name_en', 'location_name',
        'landmark_cat_name_en',
        'event_partition_date', 'start_of_week', 'start_of_month'
    )

    return output_df


def _massive_processing_daily(data_frame: DataFrame,
                              column: str,
                              config_params: str,
                              func_name: callable,
                              add_col=True,
                              config_params2=None,
                              func_name_step2=None
                              ) -> DataFrame:
    def _divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select(column).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(_divide_chunks(mvv_array, config_params.get("partition_num_per_job", 1)))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(column).isin(*[curr_item]))
        small_df = add_event_week_and_month_from_yyyymmdd(small_df, column) if add_col else small_df
        output_df = func_name(small_df, config_params)
        CNTX = load_context(Path.cwd(), env=conf)
        CNTX.catalog.save(config_params["output_catalog"], output_df)
        if func_name_step2 is not None:
            output_df2 = func_name_step2(small_df, config_params2)
            CNTX = load_context(Path.cwd(), env=conf)
            CNTX.catalog.save(config_params2["output_catalog"], output_df2)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(column).isin(*[first_item]))
    return_df = add_event_week_and_month_from_yyyymmdd(return_df, column) if add_col else return_df
    return_df = func_name(return_df, config_params)
    if func_name_step2 is not None:
        return_df2 = func_name_step2(return_df, config_params2)
        return return_df2
    return return_df


def _massive_processing_with_join_daily(data_frame: DataFrame,
                                        column: str,
                                        join_frame: DataFrame,
                                        config_params: str,
                                        func_name: callable,
                                        add_col=True,
                                        cust_frame=None
                                        ) -> DataFrame:
    def _divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select(column).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(_divide_chunks(mvv_array, config_params.get("partition_num_per_job", 1)))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(column).isin(*[curr_item]))
        small_df = add_event_week_and_month_from_yyyymmdd(small_df, column) if add_col else small_df
        small_df = join_customer_profile(small_df, cust_frame, config_params) if cust_frame is not None else small_df
        output_df = func_name(small_df, join_frame, config_params)
        CNTX = load_context(Path.cwd(), env=conf)
        CNTX.catalog.save(config_params["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(column).isin(*[first_item]))
    return_df = add_event_week_and_month_from_yyyymmdd(return_df, column) if add_col else return_df
    return_df = join_customer_profile(return_df, cust_frame, config_params) if cust_frame is not None else return_df
    return_df = func_name(return_df, join_frame, config_params)
    return return_df


def join_customer_profile(input_df: DataFrame, cust_df: DataFrame, config_params: str) -> DataFrame:
    # if check_empty_dfs([cust_df]):
    #     return get_spark_empty_df()
    #
    # cust_df = data_non_availability_and_missing_check(df=cust_df,
    #                                                   grouping="daily",
    #                                                   par_col="event_partition_date",
    #                                                   target_table_name=config_params["output_catalog"])
    #
    # if check_empty_dfs([cust_df]):
    #     return get_spark_empty_df()

    input_df = input_df.withColumnRenamed('access_method_num', 'mobile_no') \
        if config_params["column_profile"] == 'access_method_num' else input_df

    list_input_column = input_df.columns
    list_input_column.remove('mobile_no')
    list_input_column.remove('event_partition_date')
    list_input_column.remove('start_of_week')
    list_input_column.remove('start_of_month')
    cust_df = cust_df.filter('sim_sequence = "MAIN"')
    output_df = input_df.join(cust_df, ['mobile_no', 'event_partition_date'], 'inner').select(
        cust_df.subscription_identifier, input_df.mobile_no, cust_df.imsi, input_df.event_partition_date,
        input_df.start_of_week, input_df.start_of_month,
        *list_input_column
    )
    return output_df


def l1_geo_total_distance_km_daily(cell_visit: DataFrame, param_config: str) -> DataFrame:
    # ----- Transformation -----
    # Drop unneeded column
    cell_visit = cell_visit.drop('cell_id', 'time_out', 'hour_in', 'hour_out')
    cell_visit = cell_visit.where(F.col("imsi").isNotNull())

    # Window for lead function
    w_lead = Window().partitionBy('imsi', 'event_partition_date').orderBy('time_in')

    # Merge cell_visit table
    cell_visit = cell_visit.withColumn('location_id_next', F.lead('location_id', 1).over(w_lead)) \
        .select('imsi', 'time_in', 'location_id_next', 'location_id', 'latitude', 'longitude', 'event_partition_date',
                'start_of_week', 'start_of_month')
    cell_visit = cell_visit.filter('location_id_next != location_id')
    cell_visit = cell_visit.drop('location_id_next')

    # Add latitude and longitude
    cell_visit_lat = cell_visit.withColumn('latitude_next', F.lead('latitude', 1).over(w_lead))
    cell_visit_lat_long = cell_visit_lat.withColumn('longitude_next', F.lead('longitude', 1).over(w_lead))

    # Calculate distance
    cell_visit_distance = cell_visit_lat_long.withColumn('distance_km',
                                                         F.when(cell_visit_lat_long.latitude_next.isNull(), 0.00)
                                                         .otherwise(
                                                             distance_calculate_statement('latitude',
                                                                                           'longitude',
                                                                                           'latitude_next',
                                                                                           'longitude_next')))

    cell_visit_distance = cell_visit_distance.drop('latitude_next').drop('longitude_next')
    output_df = node_from_config(cell_visit_distance, param_config)

    return output_df


def l1_geo_visit_ais_store_location_daily(timespent_df: DataFrame, shape_df: DataFrame, config_param) -> DataFrame:
    output_df = timespent_df.join(shape_df, [timespent_df.location_id == shape_df.geo_shape_id], 'inner') \
        .select('imsi', 'location_id', 'landmark_name_th', 'landmark_sub_name_en',
                'landmark_latitude', 'landmark_longitude', 'num_visit', 'duration',
                'event_partition_date', 'start_of_week', 'start_of_month').dropDuplicates()

    return output_df


def l1_geo_top3_voice_location_daily(usagevoice_df: DataFrame, master_df: DataFrame, config_param: str) -> DataFrame:
    usagevoice_df = usagevoice_df.filter("service_type in ('VOICE','VOLTE')")
    join_df = usagevoice_df.join(master_df, ['lac', 'ci'], 'inner')\
        .groupBy('subscription_identifier', 'mobile_no', 'imsi',
                 'location_id', 'latitude', 'longitude',
                 'event_partition_date', 'start_of_week', 'start_of_month').agg(
        F.sum(F.col('no_of_call') + F.col('no_of_inc')).alias('total_call'),
        F.sum(F.col('no_of_call_minute') + F.col('no_of_inc_minute')).alias('total_call_minute')
    )

    return join_df


def l1_geo_data_session_location_daily(input_df: DataFrame, master_df: DataFrame, param_config) -> DataFrame:
    input_df = input_df.withColumn('week_type', F.when(
        ((F.dayofweek(F.col('event_partition_date')) == 1) | (F.dayofweek(F.col('event_partition_date')) == 7)),
        'weekend').otherwise('weekday'))

    def _sum_usage_date_statement(input_params: str) -> Column:
        return (
            F.when(
                F.lower(F.col('gprs_type')).like("%{}%".format(input_params)),
                F.sum(F.col('vol_uplink_kb') + F.col('vol_downlink_kb'))
            ).otherwise(0)
        ).alias('vol_{}'.format(input_params))

    output_df = input_df.groupBy('subscription_identifier', 'mobile_no', 'imsi',
                                 'event_partition_date', 'start_of_week', 'start_of_month', 'week_type',
                                 'lac', 'ci', 'gprs_type').agg(
        F.sum('no_of_call').alias('no_of_call'),
        F.sum('total_minute').alias('total_minute'),
        F.sum(F.col('vol_downlink_kb') + F.col('vol_uplink_kb')).alias('vol_all'),
        _sum_usage_date_statement('3g'),
        _sum_usage_date_statement('4g'),
        _sum_usage_date_statement('5g')
    )

    result_df = output_df.join(master_df, ['lac', 'ci'], 'inner') \
        .select('subscription_identifier', 'mobile_no', 'imsi',
                'event_partition_date', 'start_of_week', 'start_of_month', 'week_type',
                master_df.location_id, master_df.latitude, master_df.longitude,
                'gprs_type', 'no_of_call', 'total_minute',
                'vol_all', 'vol_3g', 'vol_4g', 'vol_5g').dropDuplicates()

    output_df = result_df.groupBy('subscription_identifier', 'mobile_no', 'imsi',
                                  'event_partition_date', 'start_of_week', 'start_of_month', 'week_type'
                                  , 'location_id', 'latitude', 'longitude'
                                  ).agg(
        F.sum('no_of_call').alias('no_of_call'),
        F.sum('total_minute').alias('total_minute'),
        F.sum('vol_all').alias('vol_all'),
        F.sum('vol_3g').alias('vol_3g'),
        F.sum('vol_4g').alias('vol_4g'),
        F.sum('vol_5g').alias('vol_5g')
    )

    return output_df


def massive_processing_with_l1_geo_visit_ais_store_location_daily(timespent_df: DataFrame,
                                                                  shape_df: DataFrame,
                                                                  config_param: str
                                                                  ) -> DataFrame:
    if check_empty_dfs([timespent_df]):
        return get_spark_empty_df()

    timespent_df = data_non_availability_and_missing_check(df=timespent_df,
                                                           grouping="daily",
                                                           par_col="event_partition_date",
                                                           target_table_name="l1_geo_visit_ais_store_location_daily",
                                                           missing_data_check_flg='N')

    if check_empty_dfs([timespent_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(timespent_df,
                                                    'event_partition_date',
                                                    shape_df,
                                                    config_param,
                                                    l1_geo_visit_ais_store_location_daily,
                                                    add_col=False)
    return output_df


def massive_processing_with_l1_geo_time_spent_by_location_daily(cust_visit_df: DataFrame,
                                                                config_param: str
                                                                ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_time_spent_by_location_daily",
                                                            missing_data_check_flg='N')

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          'partition_date',
                                          config_param,
                                          node_from_config)
    return output_df


def massive_processing_with_l1_geo_time_spent_by_store_daily(timespent_df: DataFrame,
                                                             master_df: DataFrame,
                                                             config_param: str
                                                             ) -> DataFrame:
    if check_empty_dfs([timespent_df, master_df]):
        return get_spark_empty_df()

    timespent_df = data_non_availability_and_missing_check(df=timespent_df,
                                                           grouping="daily",
                                                           par_col="event_partition_date",
                                                           target_table_name="l1_geo_time_spent_by_store_daily",
                                                           missing_data_check_flg='N')

    if check_empty_dfs([timespent_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(timespent_df,
                                                    'event_partition_date',
                                                    master_df,
                                                    config_param,
                                                    l1_geo_time_spent_by_store_daily,
                                                    add_col=False)
    return output_df


def massive_processing_with_l1_geo_count_visit_by_location_daily(cust_visit_df: DataFrame,
                                                                 config_param: str
                                                                 ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_count_visit_by_location_daily",
                                                            missing_data_check_flg='N')

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          'partition_date',
                                          config_param,
                                          node_from_config)
    return output_df


def massive_processing_with_l1_geo_total_distance_km_daily(cust_visit_df: DataFrame,
                                                           config_param: str
                                                           ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_total_distance_km_daily",
                                                            missing_data_check_flg='N')

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          'partition_date',
                                          config_param,
                                          l1_geo_total_distance_km_daily)
    return output_df


def massive_processing_with_l1_geo_count_data_session_by_location_daily(input_df: DataFrame,
                                                                        config_param: str
                                                                        ) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="daily",
                                                       par_col="event_partition_date",
                                                       target_table_name="l1_geo_count_data_session_by_location_daily",
                                                       missing_data_check_flg='N')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(input_df,
                                          'event_partition_date',
                                          config_param,
                                          node_from_config,
                                          add_col=False)
    return output_df


def massive_processing_with_l1_geo_top3_voice_location_daily(usagevoice_df: DataFrame,
                                                             master_df: DataFrame,
                                                             cust_df: DataFrame,
                                                             config_param: str
                                                             ) -> DataFrame:
    if check_empty_dfs([usagevoice_df, master_df, cust_df]):
        return get_spark_empty_df()

    usagevoice_df = data_non_availability_and_missing_check(df=usagevoice_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_top3_voice_location_daily",
                                                            missing_data_check_flg='N')

    cust_df = data_non_availability_and_missing_check(df=cust_df,
                                                      grouping="daily",
                                                      par_col="event_partition_date",
                                                      target_table_name="l1_geo_top3_voice_location_daily",
                                                      missing_data_check_flg='N')

    master_df = get_max_date_from_master_data(master_df, 'partition_date')

    min_value = union_dataframes_with_missing_cols(
        [
            usagevoice_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    usagevoice_df = usagevoice_df.filter(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(F.col("event_partition_date") <= min_value)

    if check_empty_dfs([usagevoice_df, cust_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(usagevoice_df,
                                                    'partition_date',
                                                    master_df,
                                                    config_param,
                                                    l1_geo_top3_voice_location_daily,
                                                    cust_frame=cust_df)
    return output_df


def massive_processing_with_l1_geo_data_session_location_daily(usagedata_df: DataFrame,
                                                               master_df: DataFrame,
                                                               cust_df: DataFrame,
                                                               config_param: str
                                                               ) -> DataFrame:
    if check_empty_dfs([usagedata_df, master_df, cust_df]):
        return get_spark_empty_df()

    usagedata_df = data_non_availability_and_missing_check(df=usagedata_df,
                                                           grouping="daily",
                                                           par_col="partition_date",
                                                           target_table_name="l1_geo_data_session_location_daily",
                                                           missing_data_check_flg='N')

    cust_df = data_non_availability_and_missing_check(df=cust_df,
                                                      grouping="daily",
                                                      par_col="event_partition_date",
                                                      target_table_name="l1_geo_data_session_location_daily",
                                                      missing_data_check_flg='N')

    master_df = get_max_date_from_master_data(master_df, 'partition_date')

    min_value = union_dataframes_with_missing_cols(
        [
            usagedata_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    usagedata_df = usagedata_df.filter(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(F.col("event_partition_date") <= min_value)

    if check_empty_dfs([usagedata_df, cust_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(usagedata_df,
                                                    'partition_date',
                                                    master_df,
                                                    config_param,
                                                    l1_geo_data_session_location_daily,
                                                    cust_frame=cust_df)

    return output_df



