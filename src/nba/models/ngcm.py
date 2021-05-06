"""Ingests external models for NGCM"""
import codecs
import json
import os
import pickle
from datetime import datetime
from typing import List, Union

from lightgbm import LGBMClassifier, LGBMRegressor


class Ingester:
    """Adapts McKinsey models into a format consumable by NGCM

    The models will be saved to a specified path. These files will have to be
    transferred to the correct NGCM input folder.
    """

    DAY_OF_MONTH = "Day of Month"
    """Constant specifying dynamic profile field for day of Month"""

    DAY_OF_WEEK = "Day of Week"
    """Constant specifying dynamic profile field for day of Week (1 - 7)"""

    def __init__(self, output_folder: str = None):
        """Initializes the ingester

        Parameters
        ----------
        output_folder : str
            Path to save the models
        """
        dump_path = os.path.join(output_folder, "models")
        if not os.path.exists(dump_path):
            os.makedirs(dump_path, exist_ok=True)

        self.folder = dump_path

    def ingest(
        self, model: Union[LGBMClassifier, LGBMRegressor], tag: str, features: List[str]
    ) -> str:
        """Ingests the supplied model

        The features must be set up in NGCM and also specified in the correct
        order (with respect to the model).

        Parameters
        ----------
        model: Union[LGBMClassifier, LGBMRegressor]
            The trained model
        tag: str
            An identifier used to refer to the model
        features: List[str]
            List of features used by the model in the correct order.
        """
        if not isinstance(model, (LGBMRegressor, LGBMClassifier)):
            raise TypeError("Model type %s not supported" % (type(model)))

        model = codecs.encode(pickle.dumps(model), "base64").decode()

        record = dict(features=list(features), pkl=model)

        create_time = datetime.now().strftime("%Y%m%d%H%M%S")
        file_path = os.path.join(self.folder, "%s_%s.json" % (tag, create_time))
        with open(file_path, "w") as output_file:
            json.dump(record, output_file)

        return f"Model Dumped to {file_path}"
