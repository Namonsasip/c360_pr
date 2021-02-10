from functools import partial
from kedro.pipeline import Pipeline, node

from predormancy.model_input.model_nodes import train_predormancy_model


def create_predorm_model_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(train_predormancy_model, train_date="20200210"),
                inputs={"l5_predorm_master_table": "l5_predorm_master_table",},
                outputs="unused_memory",
                name="train_predormancy_model",
                tags=["train_predormancy_model"],
            ),
        ]
    )
