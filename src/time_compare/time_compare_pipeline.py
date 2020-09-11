from kedro.pipeline import Pipeline, node
from time_compare.time_compare_nodes import direct_load_node, hive_load_node
def direct_load_pipeline() -> Pipeline:


    return Pipeline(
        [
            node(
                direct_load_node,
                inputs=[
                    "l3_customer_profile_union_monthly_feature",
                    "l4_usage_prepaid_postpaid_daily_features"
                ],
                outputs=None,
                name="direct_load_node",
                tags=["direct_load_node"],
            ),
        ]
    )

def hive_load_pipeline() -> Pipeline:


    return Pipeline(
        [
            node(
                hive_load_node,
                inputs=[
                ],
                outputs=None,
                name="hive_load_node",
                tags=["hive_load_node"],
            ),
        ]
    )
