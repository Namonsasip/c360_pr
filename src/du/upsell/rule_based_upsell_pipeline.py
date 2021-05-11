from functools import partial
from kedro.pipeline import Pipeline, node



PROD_SCHEMA_NAME = "prod_dataupsell"
DEV_SCHEMA_NAME = "dev_dataupsell"


def create_du_rule_based_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [])
