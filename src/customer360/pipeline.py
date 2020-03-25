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
from typing import Dict

from kedro.pipeline import Pipeline


from typing import Dict

from kedro.pipeline import Pipeline

from nba.report.pipelines.campaign_importance_volume_pipeline import campaign_importance_volume
from nba.report.pipelines.report_pipeline import create_use_case_view_report_data
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l1.to_l1_pipeline import (
    billing_to_l1_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l2.to_l2_pipeline import (
    billing_to_l2_pipeline,
)
from customer360.pipelines.data_engineering.pipelines.billing_pipeline.to_l3.to_l3_pipeline import (
    billing_to_l3_pipeline,
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
from .pipelines.cvm.data_prep.pipeline import (
    create_cvm_prepare_inputs_samples,
    create_cvm_prepare_data,
)
from .pipelines.cvm.modelling.pipeline import (
    create_cvm_train_model,
    create_cvm_predictions,
)
from .pipelines.cvm.preprocessing.pipeline import create_cvm_preprocessing
from .pipelines.data_engineering.pipelines.campaign_pipeline.to_l1 import (
    campaign_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.campaign_pipeline.to_l2 import (
    campaign_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.campaign_pipeline.to_l4 import (
    campaign_to_l4_pipeline,
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
# from .pipelines.data_engineering.pipelines.device_pipeline.to_l2.to_l2_pipeline import (
#     device_to_l2_pipeline,
# )
# from .pipelines.data_engineering.pipelines.device_pipeline.to_l3.to_l3_pipeline import (
#     device_to_l3_pipeline,
# )
from .pipelines.data_engineering.pipelines.revenue_pipeline.to_l3 import (
    revenue_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.revenue_pipeline.to_l4 import (
    revenue_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l1.to_l1_pipeline import (
    streaming_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l2.to_l2_pipeline import (
    streaming_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l3.to_l3_pipeline import (
    streaming_to_l3_pipeline,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l4.to_l4_pipeline import (
    streaming_to_l4_pipeline,
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
from .pipelines.data_engineering.pipelines.usage_pipeline.to_l1 import (
    usage_to_l1_pipeline,
)
from .pipelines.data_engineering.pipelines.usage_pipeline.to_l2 import (
    usage_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.usage_pipeline.to_l4 import (
    usage_to_l4_daily_pipeline,
)
from .pipelines.data_engineering.pipelines.usage_pipeline.to_l4 import (
    usage_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.device_pipeline import (
    device_to_l1_pipeline, device_to_l2_pipeline, device_to_l4_pipeline
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
        "usage_to_l4_pipeline": usage_to_l4_pipeline(),
        "customer_profile_to_l1_pipeline": customer_profile_to_l1_pipeline(),
        "customer_profile_to_l3_pipeline": customer_profile_to_l3_pipeline(),
        "customer_profile_billing_level_to_l3_pipeline": customer_profile_billing_level_to_l3_pipeline(),
        "customer_profile_to_l4_pipeline": customer_profile_to_l4_pipeline(),
        "usage_to_l1_pipeline": usage_to_l1_pipeline(),
        "billing_to_l1_pipeline": billing_to_l1_pipeline(),
        "billing_to_l3_pipeline": billing_to_l3_pipeline(),
        "billing_to_l2_pipeline": billing_to_l2_pipeline(),
        "billing_to_l4_pipeline_monthly": billing_to_l4_pipeline_monthly(),
        "billing_to_l4_pipeline_weekly": billing_to_l4_pipeline_weekly(),
        "billing_to_l4_pipeline_daily": billing_to_l4_pipeline_daily(),
        "device_to_l1_pipeline": device_to_l1_pipeline(),
        "device_to_l2_pipeline": device_to_l2_pipeline(),
        "device_to_l4_pipeline": device_to_l4_pipeline(),
        # "device_to_l3_pipeline": device_to_l3_pipeline(),
        "streaming_to_l1_pipeline": streaming_to_l1_pipeline(),
        "streaming_to_l2_pipeline": streaming_to_l2_pipeline(),
        "streaming_to_l3_pipeline": streaming_to_l3_pipeline(),
        "streaming_to_l4_pipeline": streaming_to_l4_pipeline(),
        "revenue_to_l3_pipeline": revenue_to_l3_pipeline(),
        "revenue_to_l4_pipeline": revenue_to_l4_pipeline(),
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
        "campaign_to_l4_pipeline": campaign_to_l4_pipeline(),
        # "de": data_engineering_pipeline,
    }


def create_cvm_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "cvm_prepare_inputs_dev": create_cvm_prepare_inputs_samples("dev"),
        "cvm_prepare_inputs_sample": create_cvm_prepare_inputs_samples("sample"),
        "cvm_prepare_data": create_cvm_prepare_data(None),
        "cvm_prepare_data_dev": create_cvm_prepare_data("dev"),
        "cvm_prepare_data_sample": create_cvm_prepare_data("sample"),
        "cvm_preprocessing": create_cvm_preprocessing(None),
        "cvm_preprocessing_dev": create_cvm_preprocessing("dev"),
        "cvm_preprocessing_sample": create_cvm_preprocessing("sample"),
        "cvm_train_model": create_cvm_train_model(None),
        "cvm_train_model_dev": create_cvm_train_model("dev"),
        "cvm_train_model_sample": create_cvm_train_model("sample"),
        "cvm_predict": create_cvm_predictions(None),
        "cvm_predict_dev": create_cvm_predictions("dev"),
        "cvm_predict_sample": create_cvm_predictions("sample"),
    }


def create_nba_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "__default__": campaign_importance_volume()
                        +create_use_case_view_report_data()
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
