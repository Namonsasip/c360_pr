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
    billing_to_l2_intermediate_pipeline, billing_to_l2_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l3.to_l3_pipeline import (
    billing_l1_to_l3_pipeline,
    billing_l0_to_l3_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_daily import *
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_monthly import *
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l4.to_l4_pipeline_weekly import *
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l1.to_l1_pipeline import (
    customer_profile_to_l1_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l3.to_l3_pipeline import (
    customer_profile_to_l3_pipeline,
    customer_profile_billing_level_to_l3_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.customer_profile_pipeline.to_l4.to_l4_pipeline import (
    customer_profile_to_l4_pipeline,
)
from cvm.data_prep.pipeline import training_data_prepare, scoring_data_prepare, \
    extract_features, rfe_only
from cvm.modelling.pipeline import train_model, score_model
from cvm.preprocessing.pipeline import preprocessing_fit, preprocessing_transform
from cvm.treatments.pipeline import generate_treatments
from nba.model_input.model_input_pipeline import create_nba_model_input_pipeline
from nba.models.models_pipeline import create_nba_models_pipeline
from nba.report.pipelines.campaign_importance_volume_pipeline import (
    campaign_importance_volume,
)
from nba.report.pipelines.report_pipeline import create_use_case_view_report_data
from .pipelines.data_engineering.pipelines.campaign_pipeline import (
    campaign_to_l1_pipeline,
    campaign_to_l2_pipeline,
    campaign_to_l3_pipeline,
    campaign_to_l4_pipeline,
    campaign_to_l4_ranking_pipeline
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
    digital_to_l4_weekly_pipeline,
    digital_to_l4_weekly_favourite_pipeline,
)
from .pipelines.data_engineering.pipelines.loyalty_pipeline import (
    loyalty_to_l1_pipeline,
    loyalty_to_l2_pipeline,
    loyalty_to_l4_pipeline,
    loyalty_to_l3_pipeline
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
    revenue_to_l4_daily_pipeline,
    revenue_to_l3_pipeline,
    revenue_to_l4_monthly_pipeline,
    revenue_to_l2_pipeline,
    revenue_to_l4_weekly_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l1.to_l1_pipeline import (
    streaming_to_l1_pipeline, streaming_to_l1_intermediate_pipeline
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l2.to_l2_pipeline import (
    streaming_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l3.to_l3_pipeline import (
    streaming_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l4.to_l4_pipeline import (
    streaming_l2_to_l4_pipeline,
    streaming_l1_to_l4_pipeline,
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
    usage_to_l1_pipeline,
    usage_to_l2_pipeline,
    usage_to_l3_pipeline,
    usage_to_l4_pipeline,
    usage_to_l4_daily_pipeline,
)

from .pipelines.data_engineering.pipelines.util_pipeline import (
    lineage_dependency_pipeline
)


def create_c360_pipeline(**kwargs) -> Dict[str, Pipeline]:

    return {
        # "__default__": usage_to_l1_pipeline()
        # + usage_to_l2_pipeline()
        # + usage_to_l4_pipeline()
        # + customer_profile_to_l3_pipeline()
        # + customer_profile_to_l4_pipeline()
        # + customer_profile_billing_level_to_l3_pipeline()
        # + billing_to_l1_pipeline()
        # + billing_to_l2_pipeline()
        # + billing_to_l3_pipeline()
        # + billing_to_l4_pipeline_daily()
        # + billing_to_l4_pipeline_weekly()
        # + billing_to_l4_pipeline_monthly()
        # + revenue_to_l3_pipeline()
        # + revenue_to_l4_pipeline()
        # + device_to_l1_pipeline(),
        # + device_to_l2_pipeline()
        # + device_to_l4_pipeline()
        "usage_to_l4_daily_pipeline": usage_to_l4_daily_pipeline(),
        "usage_to_l2_pipeline": usage_to_l2_pipeline(),
        "usage_to_l3_pipeline": usage_to_l3_pipeline(),
        "usage_to_l4_pipeline": usage_to_l4_pipeline(),
        "customer_profile_to_l1_pipeline": customer_profile_to_l1_pipeline(),
        "customer_profile_to_l3_pipeline": customer_profile_to_l3_pipeline(),
        "customer_profile_billing_level_to_l3_pipeline": customer_profile_billing_level_to_l3_pipeline(),
        "customer_profile_to_l4_pipeline": customer_profile_to_l4_pipeline(),
        "usage_to_l1_pipeline": usage_to_l1_pipeline(),
        "billing_to_l1_pipeline": billing_to_l1_pipeline(),
        "billing_l0_to_l3_pipeline": billing_l0_to_l3_pipeline(),
        "billing_l1_to_l3_pipeline": billing_l1_to_l3_pipeline(),
        "billing_to_l2_intermediate_pipeline": billing_to_l2_intermediate_pipeline(),
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
        # "device_to_l3_pipeline": device_to_l3_pipeline(),
        "streaming_to_l1_intermediate_pipeline": streaming_to_l1_intermediate_pipeline(),
        "streaming_to_l1_pipeline": streaming_to_l1_pipeline(),
        "streaming_to_l2_pipeline": streaming_to_l2_pipeline(),
        "streaming_to_l3_pipeline": streaming_to_l3_pipeline(),
        "streaming_l1_to_l4_pipeline": streaming_l1_to_l4_pipeline(),
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
        "loyalty_to_l4_pipeline": loyalty_to_l4_pipeline(),
        "loyalty_to_l3_pipeline": loyalty_to_l3_pipeline(),
        "network_to_l1_pipeline": network_to_l1_pipeline(),
        "network_to_l2_pipeline": network_to_l2_pipeline(),
        "network_to_l3_pipeline": network_to_l3_pipeline(),
        "network_to_l4_pipeline": network_to_l4_pipeline(),
        "product_to_l1_pipeline": product_to_l1_pipeline(),
        "product_to_l2_pipeline": product_to_l2_pipeline(),
        "product_to_l4_pipeline": product_to_l4_pipeline(),
        "lineage_dependency_pipeline": lineage_dependency_pipeline(),
        # "de": data_engineering_pipeline,
    }


def create_cvm_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "cvm_treatments": generate_treatments("scoring"),
        "cvm_full_training": training_data_prepare + preprocessing_fit()
                             + train_model(),
        "cvm_full_scoring": (
            scoring_data_prepare("scoring")
            + preprocessing_transform("scoring")
            + score_model("scoring")
            + generate_treatments("scoring")
        ),
        "cvm_full_scoring_experiment": (
            scoring_data_prepare("scoring_experiment")
            + preprocessing_transform("scoring_experiment")
            + score_model("scoring_experiment")
        ),
        "cvm_full_features_extraction": extract_features,
        "cvm_rfe_only": rfe_only,
    }


def create_nba_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "__default__": create_use_case_view_report_data()
        + create_nba_model_input_pipeline()
        + create_nba_models_pipeline()
        + campaign_importance_volume()
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
    ):
        # If many pipelines have nodes under the same modular
        # pipeline, combine the results
        if pipeline_name in all_pipelines.keys():
            all_pipelines[pipeline_name] += pipeline_object
        else:
            all_pipelines[pipeline_name] = pipeline_object

    return all_pipelines
