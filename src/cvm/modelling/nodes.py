# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import DataFrame
from typing import Dict, Any, Union
from sklearn.ensemble import RandomForestClassifier
import xgboost
import logging
from cvm.src.models.get_pandas_train_test_sample import get_pandas_train_test_sample
from cvm.src.models.predict import (
    pyspark_predict_rf,
    pyspark_predict_xgb,
)
from cvm.src.models.validate import validate_rf, log_pai_rf
from cvm.src.utils.list_targets import list_targets


def train_rf(
    df: DataFrame, parameters: Dict[str, Any]
) -> Dict[Any, Dict[Any, Dict[Union[str, Any], object]]]:
    """ Create random forest model given the table to train on.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Random forest classifier.
    """
    target_cols = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]

    log = logging.getLogger(__name__)

    def _train_for_macrosegment_target(use_case_chosen, macrosegment, target_chosen):
        log.info(
            "Training model for {} target, {} macrosegment.".format(
                target_chosen, macrosegment
            )
        )

        X, y = get_pandas_train_test_sample(
            df, parameters, target_chosen, use_case_chosen, macrosegment
        )

        rf = RandomForestClassifier(n_estimators=100, random_state=100)
        y = y.values.ravel()

        rf_fitted = rf.fit(X, y)
        rf_fitted.feature_names = list(X.columns.values)
        rf_fitted.sample_size = X.shape[0]
        return rf_fitted

    def _train_for_macrosegment(use_case_chosen, macrosegment):
        macrosegment_models = {}
        for target_chosen in target_cols[use_case_chosen]:
            macrosegment_models[target_chosen] = _train_for_macrosegment_target(
                use_case_chosen, macrosegment, target_chosen
            )
        return macrosegment_models

    def _train_for_usecase(use_chosen):
        usecase_models = {}
        for macrosegment in macrosegments[use_chosen]:
            usecase_models[macrosegment] = _train_for_macrosegment(
                use_chosen, macrosegment
            )
        return usecase_models

    models = {}
    for use_case in parameters["targets"]:
        models[use_case] = _train_for_usecase(use_case)

    return models


def train_xgb(df: DataFrame, parameters: Dict[str, Any]) -> Dict[str, xgboost.Booster]:
    """ Create xgboost models for given the table to train on and all targets.

    Args:
        df: Training preprocessed sample.
        parameters: parameters defined in parameters.yml.

    Returns:
        Xgboost classifier.
    """
    target_cols = list_targets(parameters)

    log = logging.getLogger(__name__)

    # to drop
    blacklisted = [
        "norms_net_revenue_imputed",
        "activation_region_indexed",
        "current_package_id_indexed",
        "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_others_net_revenue_monthly_last_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_imputed",
        "sum_usg_outgoing_data_volume_daily_last_fourteen_day_imputed",
        "sum_usg_outgoing_data_volume_daily_last_seven_day_imputed",
        "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_imputed",
        "sum_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_others_net_revenue_monthly_last_three_month_imputed",
        "sum_usg_outgoing_total_sms_daily_last_fourteen_day_imputed",
        "age_imputed",
        "sum_payments_top_up_volume_daily_last_ninety_day_imputed",
        "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_imputed",
        "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_usg_outgoing_total_sms_daily_last_seven_day_imputed",
        "gender_indexed",
        "sum_payments_top_ups_daily_last_ninety_day_imputed",
        "sum_rev_arpu_total_idd_net_revenue_monthly_last_month_imputed",
        "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_imputed",
        "sum_rev_arpu_total_ir_net_revenue_monthly_last_month_imputed",
        "sum_rev_arpu_total_mms_net_revenue_monthly_last_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_imputed",
        "sum_usg_outgoing_total_sms_daily_last_ninety_day_imputed",
        "sum_payments_top_up_volume_daily_last_fourteen_day_imputed",
        "sum_payments_top_up_volume_daily_last_thirty_day_imputed",
        "sum_payments_top_ups_daily_last_fourteen_day_imputed",
        "sum_payments_top_ups_daily_last_seven_day_imputed",
        "sum_payments_top_ups_daily_last_thirty_day_imputed",
        "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_ir_net_revenue_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_sms_net_revenue_monthly_last_month_imputed",
        "sum_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_sms_net_revenue_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_imputed",
        "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month_imputed",
        "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "serenade_status_indexed",
        "sum_payments_top_up_volume_daily_last_seven_day_imputed",
        "sum_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_idd_net_revenue_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_mms_net_revenue_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month_imputed",
        "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month_imputed",
        "subscription_status_indexed",
        "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month_imputed",
        "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month_imputed",
    ]
    df = df.drop(*blacklisted)

    models = {}
    for target_chosen in target_cols:
        log.info("Training xgboost model for {} target.".format(target_chosen))

        X, y = get_pandas_train_test_sample(df, parameters, target_chosen)

        xgb_model = xgboost.train(
            {"learning_rate": 0.01}, xgboost.DMatrix(X, label=y["target"]), 100
        )
        models[target_chosen] = xgb_model

    return models


def predict_xgb(
    df: DataFrame, xgb_models: Dict[str, xgboost.Booster], parameters: Dict[str, Any]
) -> DataFrame:
    """ Uses saved xgboost models to create propensity scores for given table.

    Args:
        df: Table with features.
        xgb_models: Saved dictionary of models for different targets.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with propensity scores.
    """

    predictions = pyspark_predict_xgb(df, xgb_models, parameters)
    return predictions


def predict_rf(
    df: DataFrame,
    rf_models: Dict[str, RandomForestClassifier],
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Uses saved Random Forest models to create propensity scores for given table.

    Args:
        df: Table with features.
        rf_models: Saved dictionary of models for different targets.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with propensity scores.
    """

    predictions = pyspark_predict_rf(df, rf_models, parameters)
    return predictions


def validate_log_rf(
    rf_models: Dict[str, RandomForestClassifier],
    df: DataFrame,
    parameters: Dict[str, Any],
) -> None:
    """ Validates given model on test dataset and log everything to PAI.

    Args:
        rf_models: Saved models.
        df: DataFrame to calculate metrics for, must include targets and models
          predictions.
        parameters: parameters defined in parameters.yml.
    """

    model_diags = validate_rf(df, parameters)
    log_pai_rf(rf_models, model_diags, parameters)
