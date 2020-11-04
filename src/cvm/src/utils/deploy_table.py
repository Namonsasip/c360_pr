import logging
import os
from datetime import datetime
from typing import Any, Dict

import pandas
import pytz


def deploy_table_to_path(
    table_to_save: pandas.DataFrame, parameters: Dict[str, Any], use_case: str,
):
    """ Saves given table to final output paths.

    Args:
        table_to_save: list of users and treatments in format that is ready to save.
        parameters: parameters defined in parameters.yml.
        use_case: use-case specify in export parameters.
    """
    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))

    output_path_template = parameters["export_table"][use_case]["output_path_template"]
    output_path_date_format = parameters["export_table"][use_case][
        "output_path_date_format"
    ]
    output_path_date = created_date.strftime(output_path_date_format)

    output_path = output_path_template.format(output_path_date)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    table_to_save.to_csv(output_path, index=False, header=True, sep="|")
    logging.info("Deployed table for {} to {}".format(use_case, output_path))
