import logging
import os
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from kedro.context import load_context
from pathlib import Path
from customer360.utilities.spark_util import get_spark_session
from customer360.utilities.config_parser import node_from_config, expansion
from src.customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *
from pyspark.sql.functions import countDistinct


conf = os.getenv("CONF", "base")
run_mode = os.getenv("DATA_AVAILABILITY_CHECKS", None)
log = logging.getLogger(__name__)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def gen_max_sql(data_frame, table_name, group):
    """
    Purpose: To get the max values of columns via SQL
    :param data_frame:
    :param table_name:
    :param group:
    :return:
    """
    grp_str = ', '.join(group)
    col_to_iterate = ["max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group]
    all_cols = ', '.join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(grp_str, all_cols, "from", table_name, grp_str)
    return final_str

def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]
    logging.info("Max date of master is [{0}]".format(max_date))
    input_df = input_df.where('{0}='.format(par_col) + str(max_date))
    return input_df

def union_dataframes_with_missing_cols(df_input_or_list, *args):
    """
    Purpose: To perform union of multiple dataframes(homogeneous/ heterogeneous)
    :param df_input_or_list:
    :param args:
    :return:
    """
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list] + list(args)

    col_list = set()
    for df in df_list:
        for column in df.columns:
            col_list.add(column)

    def add_missing_cols(dataframe, col_list):
        missing_cols = [column for column in col_list if column not in dataframe.columns]
        for column in missing_cols:
            dataframe = dataframe.withColumn(column, F.lit(None))
        return dataframe.select(*sorted(col_list))

    df_list_updated = [add_missing_cols(df, col_list) for df in df_list]
    return reduce(DataFrame.union, df_list_updated)


def check_empty_dfs(df_input_or_list):
    """
    Purpose: Its purpose is to check whether the input datasets are empty or not.
    You can input a single or a list of datasets. The return value would be an empty dataset if any one of the
    input dataset is empty other-wise the function will return false.
    :param df_input_or_list:
    :return:
    """
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list]

    ret_obj = False
    for df in df_list:
        if df is None:
            return True
        elif len(df.head(1)) == 0:
            return True
        else:
            pass
    return ret_obj


def execute_sql(data_frame, table_name, sql_str):
    """
    Purpose: To execute the sql statements
    :param data_frame:
    :param table_name:
    :param sql_str:
    :return:
    """
    ss = get_spark_session()
    data_frame.registerTempTable(table_name)
    return ss.sql(sql_str)


def add_start_of_week_and_month(input_df, date_column="day_id"):
    """
    Purpose: To generate date partition columns from input date column
    :param input_df:
    :param date_column:
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col(date_column))))\
        .withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col(date_column))))\
        .withColumn("event_partition_date", F.to_date(F.col(date_column)))

    return input_df


def add_event_week_and_month_from_yyyymmdd(input_df: DataFrame, column: str) -> DataFrame:
    """
    Purpose: To generate date partition columns from input date column from yyyyMMdd format
    :param input_df:
    :param column:
    :return:
    """
    input_col = column
    event_partition_date = "event_partition_date"
    input_df = input_df.withColumn("event_partition_date", F.to_date(F.col(input_col).cast(StringType()), 'yyyyMMdd'))
    input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col(event_partition_date))))
    input_df = input_df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col(event_partition_date))))

    return input_df


def __divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def _l1_join_with_customer_profile(
        input_df,
        cust_profile_df,
        config,
        current_item
) -> DataFrame:
    """
    Purpose: Common function to perform customer table join at L1 level.
    :param input_df:
    :param cust_profile_df:
    :param config:
    :param current_item:
    :return:
    """

    cust_profile_col_to_select = list(config["join_column_with_cust_profile"].keys()) + \
                                 ["start_of_week", "start_of_month", "access_method_num", "subscription_identifier"]
    cust_profile_col_to_select = list(set(cust_profile_col_to_select))  # remove duplicates

    if not isinstance(current_item[0], datetime):
        current_item = list(map(lambda x: datetime.strptime(str(x), '%Y%m%d'), current_item))

    # push down the filter to customer profile to reduce the join rows
    filtered_cust_profile_df = (cust_profile_df
                                .filter(F.col("event_partition_date").isin(current_item))
                                .select(cust_profile_col_to_select))

    return _join_with_filtered_customer_profile(
        input_df=input_df,
        filtered_cust_profile_df=filtered_cust_profile_df,
        config=config
    )


def _l2_join_with_customer_profile(
        input_df,
        cust_profile_df,
        config,
        current_item
) -> DataFrame:
    """
    Purpose: Common function to perform customer table join at L2 level.
    :param input_df:
    :param cust_profile_df:
    :param config:
    :param current_item:
    :return:
    """

    cust_profile_col_selection = set(list(config["join_column_with_cust_profile"].keys())
                                     + ["subscription_identifier"])
    # grouping all distinct customer per week
    filtered_cust_profile_df = (cust_profile_df
                                .filter(F.col("start_of_week").isin(current_item))
                                .select(*cust_profile_col_selection)
                                .distinct())

    return _join_with_filtered_customer_profile(
        input_df=input_df,
        filtered_cust_profile_df=filtered_cust_profile_df,
        config=config
    )


def _l3_join_with_customer_profile(
        input_df,
        cust_profile_df,
        config,
        current_item
) -> DataFrame:
    """
    Purpose: Common function to perform customer table join at L3 level.
    :param input_df:
    :param cust_profile_df:
    :param config:
    :param current_item:
    :return:
    """

    # Rename partition_month to start_of_month in parameter config
    config["join_column_with_cust_profile"]["start_of_month"] = config["join_column_with_cust_profile"]["partition_month"]
    del config["join_column_with_cust_profile"]["partition_month"]

    cust_profile_col_selection = set(list(config["join_column_with_cust_profile"].keys())
                                     + ["subscription_identifier"])

    filtered_cust_profile_df = (cust_profile_df
                                .withColumnRenamed("partition_month", "start_of_month")
                                .filter(F.col("start_of_month").isin(current_item)
                                        & (F.col("cust_active_this_month") == 'Y'))
                                .select(*cust_profile_col_selection))

    return _join_with_filtered_customer_profile(
        input_df=input_df,
        filtered_cust_profile_df=filtered_cust_profile_df,
        config=config
    )


def _join_with_filtered_customer_profile(
    input_df,
    filtered_cust_profile_df,
    config,
) -> DataFrame:
    """
    Purpose: Common function to perform filtered customer table join at L1 level.
    :param input_df:
    :param filtered_cust_profile_df:
    :param config:
    :return:
    """

    joined_condition = None
    for left_col, right_col in config["join_column_with_cust_profile"].items():
        condition = F.col("left.{}".format(left_col)).eqNullSafe(F.col("right.{}".format(right_col)))
        if joined_condition is None:
            joined_condition = condition
            continue

        joined_condition &= condition

    result_df = (filtered_cust_profile_df.alias("left")
                 .join(other=input_df.alias("right"),
                       on=joined_condition,
                       how="left"))

    col_to_select = []

    result_df.show(3)

    # Select all columns for right table except those used for joins
    # and exist in filtered_cust_profile_df columns
    for col in input_df.columns:
        if col in filtered_cust_profile_df.columns or \
                col in config["join_column_with_cust_profile"].values():
            continue
        col_to_select.append(F.col("right.{}".format(col)).alias(col))

    # Select all customer profile column used for joining
    for col in filtered_cust_profile_df.columns:
        col_to_select.append(F.col("left.{}".format(col)).alias(col))

    result_df = result_df.select(col_to_select)

    result_df.show(3)

    return result_df


def _massive_processing(
        input_df,
        config,
        source_partition_col="partition_date",
        sql_generator_func=node_from_config,
        cust_profile_df=None,
        cust_profile_join_func=None
) -> DataFrame:
    """
    Purpose: TO perform massive processing by dividing the massive data into small chunks to reduce load on cluster.
    :param input_df:
    :param config:
    :param source_partition_col:
    :param sql_generator_func:
    :param cust_profile_df:
    :param cust_profile_join_func:
    :return:
    """

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = config.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(source_partition_col).isin(*[curr_item]))

        output_df = sql_generator_func(small_df, config)

        if cust_profile_df is not None:
            output_df = cust_profile_join_func(input_df=output_df,
                                               cust_profile_df=cust_profile_df,
                                               config=config,
                                               current_item=curr_item)

        CNTX.catalog.save(config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(source_partition_col).isin(*[first_item]))
    return_df = sql_generator_func(return_df, config)

    if cust_profile_df is not None:
        return_df = cust_profile_join_func(input_df=return_df,
                                           cust_profile_df=cust_profile_df,
                                           config=config,
                                           current_item=first_item)

    return return_df


def l1_massive_processing(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    """
    Purpose: To perform the L1 level massive processing
    :param input_df:
    :param config:
    :param cust_profile_df:
    :return:
    """

    if not __is_valid_input_df(input_df, cust_profile_df):
        return get_spark_empty_df()

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="partition_date",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l1_join_with_customer_profile)
    return return_df


def l2_massive_processing(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    """
    Purpose: To perform the L2 level massive processing
    :param input_df:
    :param config:
    :param cust_profile_df:
    :return:
    """

    if not __is_valid_input_df(input_df, cust_profile_df):
        return get_spark_empty_df()

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="start_of_week",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l2_join_with_customer_profile)
    return return_df


def l2_massive_processing_with_expansion(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    """
    Purpose: To perform the L2 level massive processing with expansion features
    :param input_df:
    :param config:
    :param cust_profile_df:
    :return:
    """

    if not __is_valid_input_df(input_df, cust_profile_df):
        return get_spark_empty_df()

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="start_of_week",
                                    sql_generator_func=expansion,
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l2_join_with_customer_profile)
    return return_df


def l3_massive_processing(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    """
    Purpose: To perform the L3 level massive processing
    :param input_df:
    :param config:
    :param cust_profile_df:
    :return:
    """

    if not __is_valid_input_df(input_df, cust_profile_df):
        return get_spark_empty_df()

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="start_of_month",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l3_join_with_customer_profile)
    return return_df


def __is_valid_input_df(
        input_df,
        cust_profile_df
):
    """
    Valid input criteria:
    1. input_df is provided and it is not empty
    2. cust_profile_df is either:
        - provided with non empty data OR
        - not provided at all
    """
    return (input_df is not None and len(input_df.head(1)) > 0) and \
            (cust_profile_df is None or len(cust_profile_df.head(1)) > 0)


def data_non_availability_and_missing_check(df, grouping, par_col, target_table_name, missing_data_check_flg='N',
                                            exception_partitions=None):
    """
    Purpose:
    This function will check two scenario's:
        1. Whether any partition (daily/weekly/monthly) is completely missing or not.
        2. Whether any daily level data partition (only in case of weekly/monthly) is missing or not.

    In scenario's where any partition is missing then this function will process the data until
    the missing partition and ignores the rest of the data in order to maintain data quality and integrity.

    :param df: Input dataframe on which data availability checks need to be implemented.
    :param grouping:  This will tell the type of check. It can be daily, weekly or monthly.
    :param par_col: This is the partition column of input dataframe.
    :param target_table_name: This is the target table which is being referred in the source catalog
    :param missing_data_check_flg: This flag is used to tell whether data missing scenario's need to be checked or not.
                This will be "Y" only if your input dataframe has daily level data.
    :param exception_partitions: This is used to tell the function about the exception partitions that need to be
                skipped while checking for missing data.
    :return:
    """
    p_increment = str(os.getenv("RUN_INCREMENT", "yes"))
    if (p_increment == "yes"):
        logging.info("Entering data_non_availability_and_missing_check")
        if run_mode is not None:
            if (run_mode.upper() == 'UNIT_TEST') | (run_mode.upper() == 'LOCAL_FS') | (run_mode.upper() == 'NO'):
                logging.info("Exiting data_non_availability_and_missing_check as OS ENV RUN_MODE  = {}".format(run_mode))
                return df
        logging.info("Executing data_non_availability_and_missing_check IMP: OS ENV RUN_MODE NO UNIT TEST CASE SELECTED")
        spark = get_spark_session()

        if running_environment.lower() == 'on_premise':
            mtdt_tbl = spark.read.parquet('/projects/prod/c360/data/UTILITIES/metadata_table')
        else:
            mtdt_tbl = spark.read.parquet('/mnt/customer360-blob-output/C360/UTILITIES/metadata_table')

        mtdt_tbl.createOrReplaceTempView("mtdt_tbl")
        df.createOrReplaceTempView("df")

        tgt_max_date = spark.sql(
            """select to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as tgt_max_data_load_date
            from mtdt_tbl where table_name = '{0}'""".format(target_table_name))

        logging.info("initial target max date: {}".format(tgt_max_date.collect()))
        #print("initial target max date:", tgt_max_date.collect())

        ############################################################# Daily level check ########################################################
        if grouping.lower() == 'daily':
            logging.info("Entering daily level data availability checks")
            if par_col.lower() == 'partition_date':
                actual_src_partitions = spark.sql(
                    "select distinct(to_date(cast({0} as String),'yyyyMMdd')) as actual_src_partitions from df".format(
                        par_col))
                src_max_date = spark.sql(
                    "select to_date(cast(max({0}) as String),'yyyyMMdd') as src_max_date, count(distinct({0})) as No_of_partitions_in_src from df".format(
                        par_col))
                src_min_date = spark.sql(
                    "select to_date(cast(min({0}) as String),'yyyyMMdd') as src_min_date from df".format(par_col))
            else:
                actual_src_partitions = spark.sql("select distinct({0}) as actual_src_partitions from df".format(par_col))
                src_max_date = spark.sql(
                    "select max({0}) as src_max_date , count(distinct({0})) as No_of_partitions_in_src from df".format(
                        par_col))
                src_min_date = spark.sql("select min({0}) as src_min_date from df".format(par_col))

            # Creating Check Matrix:
            tgt_max_date = tgt_max_date.crossJoin(src_min_date)
            tgt_max_date = tgt_max_date.withColumn("tgt_max_date",
                                                   F.when(tgt_max_date.tgt_max_data_load_date == '1970-01-01',
                                                          F.date_sub(tgt_max_date.src_min_date, 1)).otherwise(
                                                       tgt_max_date.tgt_max_data_load_date)) \
                .drop("tgt_max_data_load_date") \
                .drop("src_min_date")

            check_matrix = tgt_max_date.crossJoin(src_max_date)
            check_matrix = check_matrix.withColumn("No_of_actual_partitions",
                                                   F.datediff(F.col("src_max_date"), F.col("tgt_max_date"))) \
                .withColumn("data_partition_missing_flag",
                            F.expr("CASE WHEN No_of_actual_partitions = No_of_partitions_in_src THEN 'N' ELSE 'Y' END"))

            logging.info("check matrix: {}".format(check_matrix.collect()))
            #print("check matrix:", check_matrix.collect())

            # Checking missing partitions:
            data_partition_missing_flag = check_matrix.select('data_partition_missing_flag').collect()[
                0].data_partition_missing_flag
            if data_partition_missing_flag == 'Y':
                logging.info("checking for missing partition")
                actual_total_partitions = check_matrix.select("No_of_actual_partitions", "tgt_max_date").withColumn(
                    "repeat", F.expr("split(repeat(',', No_of_actual_partitions), ',')"))
                actual_total_partitions = actual_total_partitions.select("*", F.posexplode("repeat").alias("daysToAdd",
                                                                                                           "dummy")).drop(
                    "repeat", "dummy", "No_of_actual_partitions") \
                    .withColumn("actual_total_partitions", F.expr("date_add(tgt_max_date, daysToAdd)")).filter(
                    "daysToAdd != 0").drop("daysToAdd", "tgt_max_date")

                missing_partitions = actual_total_partitions.join(actual_src_partitions,
                                                                  actual_total_partitions['actual_total_partitions'] ==
                                                                  actual_src_partitions['actual_src_partitions'],
                                                                  how='left')
                if exception_partitions is None or exception_partitions == []:
                    logging.info("No exception_partitions found")
                    #print("No exception_partitions found")
                    missing_partitions = missing_partitions.filter(
                        (F.col('actual_src_partitions').isNull())).withColumnRenamed("actual_total_partitions",
                                                                                     "missing_partitions")
                else:
                    logging.info("exception_partitions found as: {}".format(exception_partitions))
                    #print("exception_partitions found:", exception_partitions)
                    missing_partitions = missing_partitions.filter((F.col('actual_src_partitions').isNull()) & (
                        ~F.col('actual_total_partitions').isin(exception_partitions))) \
                        .withColumnRenamed("actual_total_partitions", "missing_partitions")


                logging.info("Data is not found in source for these missing partitions: {}".format(missing_partitions.select("missing_partitions").collect()))
                #print("missing partitions:", missing_partitions.select("missing_partitions").collect())

                min_missing_partition = missing_partitions.select(F.min(F.col("missing_partitions")).alias("min_missing_partition")).collect()[
                    0].min_missing_partition

                if min_missing_partition is None:
                    logging.info("No missing partitions found after considering the exception partitions")
                    #print("No missing partitions found after excemption check")
                else:
                    logging.info("Getting the data before missing partitions from source dataframe for further processing")
                    if par_col.lower() == 'partition_date':
                        df = df.filter(F.to_date(F.col(par_col).cast(StringType()), 'yyyyMMdd') < min_missing_partition)
                    else:
                        df = df.filter(F.col(par_col) < min_missing_partition)

            else:
                logging.info("No missing daily partitions found, returning the Dataframe")
                #print("No missing partitions found")

            return df


        ############################################################# Weekly level check ########################################################

        elif grouping.lower() == 'weekly':
            logging.info("Entering weekly level data availability checks")
            if par_col == 'partition_date':
                df = df.withColumn("start_of_week_new", F.to_date(
                    F.date_trunc('week', F.to_date((F.col(par_col)).cast(StringType()), 'yyyyMMdd'))))
                df.createOrReplaceTempView("df")
                actual_src_partitions = spark.sql("select distinct(start_of_week_new) as actual_src_partitions from df")
            else:
                df = df.withColumn("start_of_week_new", F.to_date(F.date_trunc('week', F.col(par_col))))
                df.createOrReplaceTempView("df")
                actual_src_partitions = spark.sql(
                    "select distinct(start_of_week_new) as actual_src_partitions from df".format(par_col))

            # Creating Check Matrix:
            tgt_max_date = tgt_max_date.crossJoin(
                actual_src_partitions.select(F.min(F.col("actual_src_partitions")).alias("src_min_week_partition")))
            tgt_max_date = tgt_max_date.withColumn("tgt_max_date", F.when(F.col("tgt_max_data_load_date") == '1970-01-01',
                                                                          F.date_sub(F.col("src_min_week_partition"),
                                                                                     1)).otherwise(
                F.date_sub(F.date_add(F.col("tgt_max_data_load_date"), 7), 1))).drop("tgt_max_data_load_date").drop(
                "src_min_week_partition")


            check_matrix = tgt_max_date.crossJoin(
                actual_src_partitions.select(F.max(F.col("actual_src_partitions")).alias("src_max_week_partition"),
                                             F.countDistinct(F.col("actual_src_partitions")).alias(
                                                 "No_of_partitions_in_src")))
            check_matrix = check_matrix.withColumn("No_of_actual_partitions", F.ceil(
                F.datediff(F.col("src_max_week_partition"), F.col("tgt_max_date")) / 7)) \
                .withColumn("data_partition_missing_flag",
                            F.expr("CASE WHEN No_of_actual_partitions = No_of_partitions_in_src THEN 'N' ELSE 'Y' END"))

            logging.info("check matrix: {}".format(check_matrix.collect()))

            # Checking missing partitions:
            data_partition_missing_flag = check_matrix.select('data_partition_missing_flag').collect()[
                0].data_partition_missing_flag
            if data_partition_missing_flag == 'Y':
                logging.info("checking for missing weekly partition because data_partition_missing_flag in check matrix is Y")
                actual_total_partitions = check_matrix.select("No_of_actual_partitions",
                                                              F.date_sub(F.date_add(F.col("tgt_max_date"), 1), 7).alias(
                                                                  "tgt_max_date")).withColumn("repeat", F.expr("split(repeat(',', No_of_actual_partitions), ',')"))
                actual_total_partitions = actual_total_partitions.select("*", F.posexplode("repeat").alias("daysToAdd",
                                                                                                           "dummy")).drop(
                    "repeat", "dummy", "No_of_actual_partitions") \
                    .withColumn("actual_total_partitions", F.expr("date_add(tgt_max_date, daysToAdd*7)")).filter(
                    "daysToAdd != 0").drop("daysToAdd", "tgt_max_date")

                logging.info("actual weekly partitions in source data required: {}".format(actual_total_partitions.collect()))
                missing_partitions = actual_total_partitions.join(actual_src_partitions,
                                                                  actual_total_partitions['actual_total_partitions'] ==
                                                                  actual_src_partitions['actual_src_partitions'],
                                                                  how='left')
                if exception_partitions is None or exception_partitions == []:
                    logging.info("No exception_partitions found")
                    missing_partitions = missing_partitions.filter(
                        (F.col('actual_src_partitions').isNull())).withColumnRenamed("actual_total_partitions",
                                                                                     "missing_partitions")
                else:
                    logging.info("exception_partitions found as: {} ".format(exception_partitions))
                    missing_partitions = missing_partitions.filter((F.col('actual_src_partitions').isNull()) & (
                        ~F.col('actual_total_partitions').isin(exception_partitions))) \
                        .withColumnRenamed("actual_total_partitions", "missing_partitions")

                logging.info("Data is not found in source for these weekly partitions: {}".format(missing_partitions.select("missing_partitions").collect()))

                min_missing_partition = missing_partitions.select(F.min(F.col("missing_partitions")).alias("min_missing_partition")).collect()[
                    0].min_missing_partition

                if min_missing_partition is None:
                    logging.info("No missing partitions found after considering the exception partitions")
                else:
                    logging.info("Getting the data before missing weekly partitions from source dataframe for further processing")
                    df = df.filter(F.col("start_of_week_new") < min_missing_partition)

            else:
                logging.info("No missing weekly partitions found")

            if missing_data_check_flg.upper() == 'Y':
                logging.info("checking for missing data partitions within weekly partition because missing_data_check_flg is Y")
                missing_data_partition = df.groupBy("start_of_week_new").agg(
                    countDistinct(F.col(par_col)).alias("count_of_data_partitions"))
                if exception_partitions is None or exception_partitions == []:
                    logging.info("No exception_partitions found")
                    missing_data_partition = missing_data_partition.filter(F.col("count_of_data_partitions") != 7).select(
                        F.min(F.col("start_of_week_new")).alias("start_of_week_new")).collect()[0].start_of_week_new
                else:
                    logging.info("Exception partition found as : {}".format(exception_partitions))
                    missing_data_partition = missing_data_partition.filter((F.col("count_of_data_partitions") != 7) & (
                        ~F.col('start_of_week_new').isin(exception_partitions))).select(
                        F.min(F.col("start_of_week_new")).alias("start_of_week_new")).collect()[0].start_of_week_new

                if missing_data_partition is None or missing_data_partition == [] or missing_data_partition == '':
                    logging.info("No missing data partitions found within weekly partitions")
                    df = df
                else:
                    logging.info("Few data partitions are not found in source data for these weekly partitions: {}".format(
                        missing_data_partition))
                    logging.info(
                        "Getting the data before minimum missing data partitions from source dataframe for further processing")
                    df = df.filter(F.col("start_of_week_new") < missing_data_partition)

            else:
                logging.info("Skipping missing_data_partition check because missing_data_partition_flg is not 'Y'")

            logging.info("Returning the dataframe after all data availability weekly level checks")
            df = df.drop("start_of_week_new")
            return df





        ############################################################# Monthly level check ########################################################

        elif grouping.lower() == 'monthly':
            logging.info("Entering monthly level data availability checks")
            if par_col == 'partition_date':
                df = df.withColumn("start_of_month_new", F.to_date(
                    F.date_trunc('month', F.to_date((F.col(par_col)).cast(StringType()), 'yyyyMMdd'))))
                df.createOrReplaceTempView("df")
                actual_src_partitions = spark.sql("select distinct(start_of_month_new) as actual_src_partitions from df")

            elif par_col == 'partition_month':
                df = df.withColumn("start_of_month_new", F.to_date(
                    F.date_trunc('month', F.to_date((F.col(par_col)).cast(StringType()), 'yyyyMM'))))
                df.createOrReplaceTempView("df")
                actual_src_partitions = spark.sql("select distinct(start_of_month_new) as actual_src_partitions from df")

            else:
                df = df.withColumn("start_of_month_new", F.to_date(F.date_trunc('month', F.col(par_col))))
                df.createOrReplaceTempView("df")
                actual_src_partitions = spark.sql(
                    "select distinct(start_of_month_new) as actual_src_partitions from df".format(par_col))

            # Creating Check Matrix:
            tgt_max_date = tgt_max_date.crossJoin(
                actual_src_partitions.select(F.min(F.col("actual_src_partitions")).alias("src_min_month_partition")))
            tgt_max_date = tgt_max_date.withColumn("tgt_max_date", F.when(F.col("tgt_max_data_load_date") == '1970-01-01',
                                                                          F.date_sub(F.col("src_min_month_partition"),
                                                                                     1)).otherwise(
                F.date_sub(F.add_months(F.col("tgt_max_data_load_date"), 1), 1))).drop("tgt_max_data_load_date").drop(
                "src_min_week_partition")


            check_matrix = tgt_max_date.crossJoin(
                actual_src_partitions.select(F.max(F.col("actual_src_partitions")).alias("src_max_month_partition"),
                                             F.countDistinct(F.col("actual_src_partitions")).alias(
                                                 "No_of_partitions_in_src")))
            check_matrix = check_matrix.withColumn("No_of_actual_partitions", F.ceil(
                F.months_between(F.col("src_max_month_partition"), F.col("tgt_max_date")))) \
                .withColumn("data_partition_missing_flag",
                            F.expr("CASE WHEN No_of_actual_partitions = No_of_partitions_in_src THEN 'N' ELSE 'Y' END"))

            logging.info("check matrix: {}".format(check_matrix.collect()))
            #print("check matrix:", check_matrix.collect())

            # Checking missing partitions:
            data_partition_missing_flag = check_matrix.select('data_partition_missing_flag').collect()[
                0].data_partition_missing_flag
            if data_partition_missing_flag == 'Y':
                logging.info("checking for missing monthly partition because data_partition_missing_flag in check matrix is Y")
                actual_total_partitions = check_matrix.select("No_of_actual_partitions",
                                                              F.add_months(F.date_add(F.col("tgt_max_date"), 1), -1).alias(
                                                                  "tgt_max_date")).withColumn("repeat", F.expr("split(repeat(',', No_of_actual_partitions), ',')"))
                actual_total_partitions = actual_total_partitions.select("*", F.posexplode("repeat").alias("monthsToAdd",
                                                                                                           "dummy")).drop(
                    "repeat", "dummy", "No_of_actual_partitions") \
                    .withColumn("actual_total_partitions", F.expr("add_months(tgt_max_date, monthsToAdd)")).filter(
                    "monthsToAdd != 0").drop("monthsToAdd", "tgt_max_date")

                logging.info("actual monthly partitions in source data: {}".format(actual_total_partitions.collect()))
                #print("actual_total_partitions:", actual_total_partitions.collect())
                missing_partitions = actual_total_partitions.join(actual_src_partitions,
                                                                  actual_total_partitions['actual_total_partitions'] ==
                                                                  actual_src_partitions['actual_src_partitions'],
                                                                  how='left')
                if exception_partitions is None or exception_partitions == []:
                    logging.info("No exception_partitions found")
                    #print("No exception_partitions found")
                    missing_partitions = missing_partitions.filter(
                        (F.col('actual_src_partitions').isNull())).withColumnRenamed("actual_total_partitions",
                                                                                     "missing_partitions")
                else:
                    logging.info("exception_partitions found as : {}".format(exception_partitions))
                    #print("exception_partitions found:", exception_partitions)
                    missing_partitions = missing_partitions.filter((F.col('actual_src_partitions').isNull()) & (
                        ~F.col('actual_total_partitions').isin(exception_partitions))) \
                        .withColumnRenamed("actual_total_partitions", "missing_partitions")

                logging.info("Data is not found in source for these monthly partitions: {}".format(missing_partitions.select("missing_partitions").collect()))
                #print("missing partitions:", missing_partitions.select("missing_partitions").collect())

                min_missing_partition = missing_partitions.select(F.min(F.col("missing_partitions")).alias("min_missing_partition")).collect()[
                    0].min_missing_partition

                if min_missing_partition is None:
                    logging.info("No missing monthly partitions found after considering exception partitions")
                    #print("No missing partitions found after excemption check")
                else:
                    logging.info(
                        "Getting the data before missing monthly partitions from source dataframe for further processing")
                    df = df.filter(F.col("start_of_month_new") < min_missing_partition)

            else:
                logging.info("No missing monthly partitions found")
                #print("No missing partitions found")

            if missing_data_check_flg.upper() == 'Y':
                logging.info(
                    "checking for missing data partitions within monthly partition because missing_data_check_flg is Y")
                missing_data_partition = df.groupBy("start_of_month_new").agg(
                    countDistinct(F.col(par_col)).alias("count_of_src_data_partitions"))
                missing_data_partition = missing_data_partition.withColumn("count_of_actual_data_partitions", F.datediff(
                    F.add_months(F.col("start_of_month_new"), 1), F.col("start_of_month_new")))
                if exception_partitions is None or exception_partitions == []:
                    logging.info("No exception_partitions found")
                    #print("No exception_partitions found")
                    missing_data_partition = missing_data_partition.filter(
                        F.col("count_of_src_data_partitions") != F.col("count_of_actual_data_partitions")).select(
                        F.min(F.col("start_of_month_new")).alias("start_of_month_new")).collect()[0].start_of_month_new
                else:
                    logging.info("Exception partitions found as : {}".format(exception_partitions))
                    #print("Exception partition found:", exception_partitions)
                    missing_data_partition = missing_data_partition.filter(
                        (F.col("count_of_src_data_partitions") != F.col("count_of_actual_data_partitions")) & (
                            ~F.col('start_of_month_new').isin(exception_partitions))).select(
                        F.min(F.col("start_of_month_new")).alias("start_of_month_new")).collect()[0].start_of_month_new


                if missing_data_partition is None or missing_data_partition == [] or missing_data_partition == '':
                    logging.info("No missing data partitions found within monthly partitions")
                    #print("No missing data partitions found")
                    df = df
                else:
                    logging.info(
                        "Few missing data partitions are not found in source data for these monthly partitions: {}".format(
                            missing_data_partition))
                    logging.info(
                        "Getting the data before minimum missing data partitions from source dataframe for further processing")
                    df = df.filter(F.col("start_of_month_new") < missing_data_partition)

            else:
                logging.info("Skipping missing_data_partition check because missing_data_partition_flg is not 'Y'")

            logging.info("Returning the dataframe after all data availability monthly level checks")
            df = df.drop("start_of_month_new")
            return df
    else :
        return df