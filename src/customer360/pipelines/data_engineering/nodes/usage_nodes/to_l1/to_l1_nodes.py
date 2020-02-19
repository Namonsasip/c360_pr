from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path


def gen_max_sql(data_frame, table_name, group):
    grp_str = ', '.join(group)
    col_to_iterate = ["max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group]
    all_cols = ', '.join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(grp_str, all_cols, "from", table_name, grp_str)
    return final_str


def massive_processing(input_df, sql, output_df_catalog):
    """
    :return:
    """
    CNTX = load_context(Path.cwd(), env='base')
    data_frame = input_df
    dates_list = data_frame.select('partition_date').distinct().collect()

    mvv_array = [row[0] for row in dates_list]
    # if len(mvv_array) != 1 and len(mvv_array) % 2 != 0:
    #     mvv_array.append(mvv_array[0])
    # add_list = []
    # if len(mvv_array) > 1:
    #     final_list_proceed = zip(mvv_array[::2], mvv_array[1::2])
    #     for i, j in final_list_proceed:
    #         add_list.append((i, j))
    # else:
    #     add_list = mvv_array
    #
    # # for first item
    # first_item = add_list[0]
    # if len(mvv_array) > 1:
    #     return_df = data_frame.filter((F.col("partition_date") == first_item[0]) |
    #                                   (F.col("partition_date") == first_item[1]))
    #     return_df = node_from_config(return_df, sql)
    #
    #     # for rest of dfs
    #     add_list.remove(first_item)
    #     for curr_item in add_list:
    #         small_df = data_frame.filter((F.col("partition_date") == curr_item[0]) |
    #                                      (F.col("partition_date") == curr_item[1]))
    #         output_df = node_from_config(small_df, sql)
    #         CNTX.catalog.save(output_df_catalog, output_df)
    #
    # else:
    #     return_df = data_frame.filter((F.col("partition_date") == first_item))
    #     return_df = node_from_config(return_df, sql)
    first_item = mvv_array[0]
    return_df = data_frame.filter((F.col("partition_date") == first_item))
    return_df = node_from_config(return_df, sql)

    mvv_array.remove(first_item)
    for curr_item in mvv_array:
        small_df = data_frame.filter(F.col("partition_date") == curr_item)
        output_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    return return_df


def merge_with_customer_df(source_df: DataFrame,
                           cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'subscription_identifier', 'event_partition_date']
    join_key = ['access_method_num', 'event_partition_date']
    final_df = source_df.join(cust_df.select(cust_df_cols), join_key, how="left")

    return final_df


def usage_outgoing_ir_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_outgoing_call_relation_sum_ir_daily")
    return return_df


def usage_incoming_ir_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_incoming_call_relation_sum_ir_daily")
    return return_df


def usage_outgoing_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_outgoing_call_relation_sum_daily")
    return return_df


def usage_incoming_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_incoming_call_relation_sum_daily")
    return return_df


def usage_data_prepaid_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_ru_a_gprs_cbs_usage_daily")
    return return_df


def usage_data_postpaid_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "l1_usage_ru_a_vas_postpaid_usg_daily")
    return return_df


def build_data_for_prepaid_postpaid_vas(prepaid: DataFrame
                                        , postpaid: DataFrame) -> DataFrame:
    """

    :param prepaid:
    :param postpaid:
    :return:
    """
    prepaid = prepaid.select("access_method_num", "number_of_call", 'day_id')
    postpaid = postpaid.where("call_type_cd = 5") \
        .select("access_method_num", F.col("no_transaction").alias("number_of_call"), 'day_id')

    final_df = union_dataframes_with_missing_cols(prepaid, postpaid)

    return final_df


def merge_all_dataset_to_one_table(l1_usage_outgoing_call_relation_sum_daily_stg: DataFrame,
                                   l1_usage_incoming_call_relation_sum_daily_stg: DataFrame,
                                   # l1_usage_outgoing_call_relation_sum_ir_daily_stg: DataFrame,
                                   # l1_usage_incoming_call_relation_sum_ir_daily_stg: DataFrame,
                                   l1_usage_ru_a_gprs_cbs_usage_daily_stg: DataFrame,
                                   l1_usage_ru_a_vas_postpaid_usg_daily_stg: DataFrame,
                                   l1_usage_ru_a_vas_postpaid_prepaid_daily_stg: DataFrame,
                                   l1_customer_profile_union_daily_feature: DataFrame) -> DataFrame:
    """
    :param l1_usage_outgoing_call_relation_sum_daily_stg:
    :param l1_usage_incoming_call_relation_sum_daily_stg:
    :param l1_usage_outgoing_call_relation_sum_ir_daily_stg:
    :param l1_usage_incoming_call_relation_sum_ir_daily_stg:
    :param l1_usage_ru_a_gprs_cbs_usage_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_usg_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_prepaid_daily_stg:
    :param l1_customer_profile_union_daily_feature:
    :return:
    """
    drop_cols = ["access_method_num", "called_no", "caller_no"]
    union_df = union_dataframes_with_missing_cols([
        l1_usage_outgoing_call_relation_sum_daily_stg, l1_usage_incoming_call_relation_sum_daily_stg,
        # l1_usage_outgoing_call_relation_sum_ir_daily_stg, l1_usage_incoming_call_relation_sum_ir_daily_stg,
        l1_usage_ru_a_gprs_cbs_usage_daily_stg, l1_usage_ru_a_vas_postpaid_usg_daily_stg,
        l1_usage_ru_a_vas_postpaid_prepaid_daily_stg
    ])

    group_cols = ['access_method_num', 'event_partition_date']
    final_df_str = gen_max_sql(union_df, 'roaming_incoming_outgoing_data', group_cols)

    """
    :return:
    """
    CNTX = load_context(Path.cwd(), env='base')
    data_frame = union_df
    dates_list = data_frame.select('event_partition_date').distinct().collect()

    mvv_array = [row[0] for row in dates_list]
    add_list = []
    if len(mvv_array) != 1 and len(mvv_array) % 2 != 0:
        mvv_array.append(mvv_array[0])
    if len(mvv_array) > 1:
        final_list_proceed = zip(mvv_array[::2], mvv_array[1::2])
        for i, j in final_list_proceed:
            add_list.append((i, j))
    else:
        add_list = mvv_array

    # for first item
    first_item = add_list[0]
    if len(mvv_array) > 1:
        return_df = data_frame.filter((F.col("event_partition_date") == first_item[0]) |
                                      (F.col("event_partition_date") == first_item[1]))
        return_df = execute_sql(data_frame=return_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)

        # for rest of dfs
        add_list.remove(first_item)
        for curr_item in add_list:
            small_df = data_frame.filter((F.col("event_partition_date") == curr_item[0]) |
                                         (F.col("event_partition_date") == curr_item[1]))
            output_df = execute_sql(data_frame=small_df, table_name='roaming_incoming_outgoing_data',
                                    sql_str=final_df_str)
            CNTX.catalog.save("l1_usage_postpaid_prepaid_daily", output_df.drop(*[drop_cols]))
    else:
        return_df = data_frame.filter((F.col("event_partition_date") == first_item))
        return_df = execute_sql(data_frame=return_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)

    return return_df.drop(*[drop_cols])
