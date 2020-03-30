# from functools import partial
#
# from kedro.pipeline import Pipeline, node
#
# from nba.model_input.model_input_nodes import (
#     node_l5_nba_master_table_spine,
#     node_l5_nba_master_table,
#     node_l5_nba_master_table_chunk_debug,
# )
#
#
# def create_nba_model_input_pipeline() -> Pipeline:
#     return Pipeline(
#         [
#             node(
#                 node_l5_nba_master_table_spine,
#                 inputs={
#                     "l0_campaign_tracking_contact_list_pre": "l0_campaign_tracking_contact_list_pre",
#                     "min_feature_days_lag": "params:nba_min_feature_days_lag",
#                 },
#                 outputs="l5_nba_master_table_spine",
#                 name="l5_nba_master_table_spine",
#                 tags=["l5_nba_master_table_spine"],
#             ),
#             node(
#                 node_l5_nba_master_table,
#                 inputs={
#                     "l5_nba_master_table_spine": "l5_nba_master_table_spine",
#                     "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
#                     "l4_campaign_top_channel_features": "l4_campaign_top_channel_features",
#                     # "l4_customer_profile_ltv_to_date": "l4_customer_profile_ltv_to_date",
#                 },
#                 outputs="l5_nba_master_table",
#                 name="l5_nba_master_table",
#                 tags=["l5_nba_master_table"],
#             ),
#             node(
#                 partial(
#                     node_l5_nba_master_table_chunk_debug,
#                     child_code="TU39.2",
#                     sampling_rate=1e-2,
#                 ),
#                 inputs={"l5_nba_master_table": "l5_nba_master_table",},
#                 outputs=[
#                     "l5_nba_master_table_chunk_debug",
#                     "master_table_chunk_debug_extra_pai_metrics",
#                 ],
#                 name="l5_nba_master_table_chunk_debug",
#                 tags=["l5_nba_master_table_chunk_debug",],
#             ),
#         ],
#         tags="nba_model_input",
#     )
