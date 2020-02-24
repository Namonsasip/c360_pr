from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import massive_processing
import os

conf = os.environ["CONF"]

def device_most_used_monthly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_month", "l3_device_most_used_monthly")
    return return_df

def device_number_of_phone_updates_monthly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_month", "l3_device_number_of_phone_updates_monthly")
    return return_df

def device_previous_configurations_monthly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_month", "l3_previous_device_handset_summary_with_configuration_monthly")
    return return_df