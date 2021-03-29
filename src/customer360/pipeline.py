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
"""Pipeline construction."""
import itertools

from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l1.to_l1_pipeline import (
    billing_to_l1_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l2.to_l2_pipeline import (
    billing_to_l2_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l3.to_l3_pipeline import (
    billing_l0_to_l3_pipeline,
    billing_l1_to_l3_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_daily import *
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_monthly import *
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_weekly import *
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l1.to_l1_pipeline import (
    customer_profile_to_l1_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l2.to_l2_pipeline import (
    customer_profile_to_l2_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l3.to_l3_pipeline import (
    customer_profile_billing_level_to_l3_pipeline,
    customer_profile_to_l3_pipeline,
    unioned_customer_profile_to_l3_pipeline,
)
from cvm.customer_id.pipeline import create_customer_id
from cvm.data_prep.pipeline import (
    create_cvm_important_columns,
    scoring_data_prepare,
    training_data_prepare,
    validation_data_prepare,
)
from cvm.modelling.pipeline import score_model, train_model, validate_model
from cvm.preprocessing.pipeline import preprocessing_fit, preprocessing_transform
from cvm.report.pipeline import create_kpis, prepare_user_microsegments, run_report
from cvm.sample_inputs.pipeline import (
    create_sub_id_mapping_pipeline,
    prepare_input_tables,
)
from cvm.treatments.pipeline import generate_treatments
from nba.backtesting.backtesting_pipeline import create_nba_backtesting_pipeline
from nba.gender_age_imputation.gender_age_imputation_pipeline import (
    create_nba_gender_age_imputation_pipeline,
)
from nba.model_input.model_input_pipeline import create_nba_model_input_pipeline
from nba.models.models_pipeline import create_nba_models_pipeline
from nba.pcm_scoring.pcm_scoring_pipeline import create_nba_pcm_scoring_pipeline
from nba.personnas_clustering.personnas_clustering_pipeline import (
    create_nba_personnas_clustering_pipeline,
)
from nba.report.pipelines.campaign_importance_volume_pipeline import (
    campaign_importance_volume,
)
from nba.report.pipelines.report_pipeline import create_use_case_view_report_data

from data_quality.pipeline import (
    data_quality_pipeline,
    subscription_id_sampling_pipeline,
    threshold_analysis_pipeline,
)

from .pipelines.data_engineering.pipelines.campaign_pipeline import (
    campaign_to_l1_pipeline,
    campaign_to_l2_pipeline,
    campaign_to_l3_pipeline,
    campaign_to_l4_pipeline,
    campaign_to_l4_ranking_pipeline,
)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l1.to_l1_pipeline import (
    complaints_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l2.to_l2_pipeline import (
    complaints_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l3.to_l3_pipeline import (
    complaints_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l4.to_l4_pipeline import (
    complaints_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.device_pipeline import (
    device_to_l1_pipeline,
    device_to_l2_pipeline,
    device_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.digital_pipeline import (
    digital_to_l2_pipeline,
    digital_to_l3_pipeline,
    digital_to_l4_monthly_pipeline,
    digital_to_l4_weekly_favourite_pipeline,
    digital_to_l4_weekly_pipeline,
)
from .pipelines.data_engineering.pipelines.loyalty_pipeline import (
    loyalty_to_l1_pipeline,
    loyalty_to_l2_pipeline,
    loyalty_to_l3_pipeline,
    loyalty_to_l4_monthly_pipeline,
    loyalty_to_l4_weekly_pipeline,
)
from .pipelines.data_engineering.pipelines.network_pipeline.to_l1.to_l1_pipeline import (
    network_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.network_pipeline.to_l2.to_l2_pipeline import (
    network_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.network_pipeline.to_l3.to_l3_pipeline import (
    network_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.network_pipeline.to_l4.to_l4_pipeline import (
    network_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.product_pipeline.to_l1.to_l1_pipeline import (
    product_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.product_pipeline.to_l2.to_l2_pipeline import (
    product_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.product_pipeline.to_l4.to_l4_pipeline import (
    product_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.revenue_pipeline import (
    revenue_to_l1_pipeline,
    revenue_to_l2_pipeline,
    revenue_to_l3_pipeline,
    revenue_to_l4_daily_pipeline,
    revenue_to_l4_monthly_pipeline,
    revenue_to_l4_weekly_pipeline,
)
from .pipelines.data_engineering.pipelines.sales_pipeline.to_l2.to_l2_pipeline import (
    sales_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.sales_pipeline.to_l4.to_l4_weekly_pipeline import (
    sales_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l1.to_l1_pipeline import (
    streaming_to_l1_onair_vimmi_pipeline,
    streaming_to_l1_session_duration_pipeline,
    streaming_to_l1_soc_mobile_data_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l2.to_l2_pipeline import (
    streaming_to_l2_intermediate_pipeline,
    streaming_to_l2_pipeline,
    streaming_to_l2_session_duration_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l3.to_l3_pipeline import (
    streaming_to_l3_pipeline,
    streaming_to_l3_session_duration_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l4.to_l4_pipeline import (
    streaming_l1_to_l4_pipeline,
    streaming_l2_to_l4_pipeline,
    streaming_l2_to_l4_session_duration_pipeline,
)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l1.to_l1_pipeline import (
    touchpoints_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l2.to_l2_pipeline import (
    touchpoints_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l3.to_l3_pipeline import (
    touchpoints_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l4.to_l4_pipeline import (
    touchpoints_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.usage_pipeline import (
    usage_create_master_data_for_favourite_feature,
    usage_to_l1_pipeline,
    usage_to_l2_pipeline,
    usage_to_l3_pipeline,
    usage_to_l4_daily_pipeline,
    usage_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.util_pipeline import (
    lineage_dependency_pipeline,
    ops_report_pipeline,
)


def create_c360_pipeline(**kwargs) -> Dict[str, Pipeline]:

    return {
        "usage_to_l1_pipeline": usage_to_l1_pipeline(),
        "usage_create_master_data_for_favourite_feature": usage_create_master_data_for_favourite_feature(),
        "usage_to_l4_daily_pipeline": usage_to_l4_daily_pipeline(),
        "usage_to_l2_pipeline": usage_to_l2_pipeline(),
        "usage_to_l3_pipeline": usage_to_l3_pipeline(),
        "usage_to_l4_pipeline": usage_to_l4_pipeline(),
        "customer_profile_to_l1_pipeline": customer_profile_to_l1_pipeline(),
        "customer_profile_to_l2_pipeline": customer_profile_to_l2_pipeline(),
        "customer_profile_to_l3_pipeline": customer_profile_to_l3_pipeline(),
        "unioned_customer_profile_to_l3_pipeline": unioned_customer_profile_to_l3_pipeline(),
        "customer_profile_billing_level_to_l3_pipeline": customer_profile_billing_level_to_l3_pipeline(),
        "billing_to_l1_pipeline": billing_to_l1_pipeline(),
        "billing_l0_to_l3_pipeline": billing_l0_to_l3_pipeline(),
        "billing_l1_to_l3_pipeline": billing_l1_to_l3_pipeline(),
        # "billing_to_l2_intermediate_pipeline": billing_to_l2_intermediate_pipeline(),
        "billing_to_l2_pipeline": billing_to_l2_pipeline(),
        "billing_to_l4_pipeline_monthly": billing_to_l4_pipeline_monthly(),
        "billing_to_l4_pipeline_weekly": billing_to_l4_pipeline_weekly(),
        "billing_to_l4_ranked_pipeline_weekly": billing_to_l4_ranked_pipeline_weekly(),
        "billing_to_l4_pipeline_daily": billing_to_l4_pipeline_daily(),
        "device_to_l1_pipeline": device_to_l1_pipeline(),
        "device_to_l2_pipeline": device_to_l2_pipeline(),
        "device_to_l4_pipeline": device_to_l4_pipeline(),
        "digital_to_l2_pipeline": digital_to_l2_pipeline(),
        "digital_to_l3_pipeline": digital_to_l3_pipeline(),
        "digital_to_l4_monthly_pipeline": digital_to_l4_monthly_pipeline(),
        "digital_to_l4_weekly_pipeline": digital_to_l4_weekly_pipeline(),
        "digital_to_l4_weekly_favourite_pipeline": digital_to_l4_weekly_favourite_pipeline(),
        "streaming_to_l1_onair_vimmi_pipeline": streaming_to_l1_onair_vimmi_pipeline(),
        "streaming_to_l1_soc_mobile_data_pipeline": streaming_to_l1_soc_mobile_data_pipeline(),
        "streaming_to_l1_session_duration_pipeline": streaming_to_l1_session_duration_pipeline(),
        "streaming_to_l2_intermediate_pipeline": streaming_to_l2_intermediate_pipeline(),
        "streaming_to_l2_pipeline": streaming_to_l2_pipeline(),
        "streaming_to_l2_session_duration_pipeline": streaming_to_l2_session_duration_pipeline(),
        "streaming_to_l3_pipeline": streaming_to_l3_pipeline(),
        "streaming_to_l3_session_duration_pipeline": streaming_to_l3_session_duration_pipeline(),
        "streaming_l1_to_l4_pipeline": streaming_l1_to_l4_pipeline(),
        "streaming_l2_to_l4_session_duration_pipeline": streaming_l2_to_l4_session_duration_pipeline(),
        "streaming_l2_to_l4_pipeline": streaming_l2_to_l4_pipeline(),
        "revenue_to_l1_pipeline": revenue_to_l1_pipeline(),
        "revenue_to_l2_pipeline": revenue_to_l2_pipeline(),
        "revenue_to_l3_pipeline": revenue_to_l3_pipeline(),
        "revenue_to_l4_daily_pipeline": revenue_to_l4_daily_pipeline(),
        "revenue_to_l4_monthly_pipeline": revenue_to_l4_monthly_pipeline(),
        "revenue_to_l4_weekly_pipeline": revenue_to_l4_weekly_pipeline(),
        "complaints_to_l1_pipeline": complaints_to_l1_pipeline(),
        "complaints_to_l2_pipeline": complaints_to_l2_pipeline(),
        "complaints_to_l3_pipeline": complaints_to_l3_pipeline(),
        "complaints_to_l4_pipeline": complaints_to_l4_pipeline(),
        "touchpoints_to_l1_pipeline": touchpoints_to_l1_pipeline(),
        "touchpoints_to_l2_pipeline": touchpoints_to_l2_pipeline(),
        "touchpoints_to_l3_pipeline": touchpoints_to_l3_pipeline(),
        "touchpoints_to_l4_pipeline": touchpoints_to_l4_pipeline(),
        "campaign_to_l1_pipeline": campaign_to_l1_pipeline(),
        "campaign_to_l2_pipeline": campaign_to_l2_pipeline(),
        "campaign_to_l3_pipeline": campaign_to_l3_pipeline(),
        "campaign_to_l4_pipeline": campaign_to_l4_pipeline(),
        "campaign_to_l4_ranking_pipeline": campaign_to_l4_ranking_pipeline(),
        "loyalty_to_l1_pipeline": loyalty_to_l1_pipeline(),
        "loyalty_to_l2_pipeline": loyalty_to_l2_pipeline(),
        "loyalty_to_l3_pipeline": loyalty_to_l3_pipeline(),
        "loyalty_to_l4_weekly_pipeline": loyalty_to_l4_weekly_pipeline(),
        "loyalty_to_l4_monthly_pipeline": loyalty_to_l4_monthly_pipeline(),
        "network_to_l1_pipeline": network_to_l1_pipeline(),
        "network_to_l2_pipeline": network_to_l2_pipeline(),
        "network_to_l3_pipeline": network_to_l3_pipeline(),
        "network_to_l4_pipeline": network_to_l4_pipeline(),
        "product_to_l1_pipeline": product_to_l1_pipeline(),
        "product_to_l2_pipeline": product_to_l2_pipeline(),
        "product_to_l4_pipeline": product_to_l4_pipeline(),
        "sales_to_l2_pipeline": sales_to_l2_pipeline(),
        "sales_to_l4_pipeline": sales_to_l4_pipeline(),
        "lineage_dependency_pipeline": lineage_dependency_pipeline(),
        "ops_report_pipeline": ops_report_pipeline(),
    }


def create_cvm_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "cvm_treatments": generate_treatments("scoring"),
        "cvm_full_training": (
            prepare_input_tables("training")
            + training_data_prepare("training")
            + preprocessing_fit("training")
            + train_model("training")
        ),
        "cvm_full_training_zero": (
                prepare_input_tables("training_zero")
                + training_data_prepare("training_zero")
                + preprocessing_fit("training_zero")
                + train_model("training_zero")
        ),
        "cvm_validation": (
                prepare_input_tables("validation")
                + validation_data_prepare("validation")
                + preprocessing_transform("validation")
                + validate_model()
        ),
        "cvm_full_scoring": (
            prepare_input_tables("scoring")
            + scoring_data_prepare("scoring")
            + preprocessing_transform("scoring")
            + score_model("scoring")
            + generate_treatments("scoring")
        ),
        "cvm_full_scoring_all": (
                prepare_input_tables("scoring_all")
                + scoring_data_prepare("scoring_all")
                + preprocessing_transform("scoring_all")
                + score_model("scoring_all")
                + generate_treatments("scoring_all")
        ),
        "cvm_full_scoring_experiment": (
            prepare_input_tables("scoring_experiment")
            + scoring_data_prepare("scoring_experiment")
            + preprocessing_transform("scoring_experiment")
            + score_model("scoring_experiment")
            + generate_treatments("scoring_experiment")
        ),
        "cvm_full_features_extraction": (
            prepare_input_tables("fe")
            + training_data_prepare("fe")
            + create_cvm_important_columns()
        ),
        "cvm_customer_id": create_customer_id(),
        "cvm_rfe_only": create_cvm_important_columns(),
        "cvm_prepare_report_micro": prepare_user_microsegments(),
        "cvm_create_kpis": create_kpis(),
        "cvm_sample_inputs": prepare_input_tables("scoring"),
        "cvm_create_sub_id_mapping": create_sub_id_mapping_pipeline(),
    }


def create_nba_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "__default__": create_use_case_view_report_data()
        + create_nba_model_input_pipeline()
        + create_nba_models_pipeline()
        + campaign_importance_volume()
        + create_nba_backtesting_pipeline()
        + create_nba_pcm_scoring_pipeline()
        + create_nba_gender_age_imputation_pipeline()
        + create_nba_personnas_clustering_pipeline()
    }


def create_dq_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "data_quality_pipeline": data_quality_pipeline(),
        "subscription_id_sampling_pipeline": subscription_id_sampling_pipeline(),
        "threshold_analysis_pipeline": threshold_analysis_pipeline(),
    }


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.
    Args:
        kwargs: Ignore any additional arguments added in the future.
    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    all_pipelines = {}

    for pipeline_name, pipeline_object in itertools.chain(
        create_c360_pipeline(**kwargs).items(),
        create_cvm_pipeline(**kwargs).items(),
        create_nba_pipeline(**kwargs).items(),
        create_dq_pipeline(**kwargs).items(),
    ):
        # If many pipelines have nodes under the same modular
        # pipeline, combine the results
        if pipeline_name in all_pipelines.keys():
            all_pipelines[pipeline_name] += pipeline_object
        else:
            all_pipelines[pipeline_name] = pipeline_object

    return all_pipelines
