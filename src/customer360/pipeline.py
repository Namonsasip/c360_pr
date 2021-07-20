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
    billing_l1_to_l3_pipeline,
    billing_l0_to_l3_pipeline,
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
    customer_profile_to_l3_pipeline,
    customer_profile_billing_level_to_l3_pipeline,
    unioned_customer_profile_to_l3_pipeline
)
from cvm.data_prep.pipeline import (
    create_cvm_prepare_inputs_samples,
    create_cvm_targets,
    create_cvm_training_data,
    create_cvm_scoring_data,
)
from cvm.modelling.pipeline import create_train_model, create_predictions
from cvm.preprocessing.pipeline import (
    create_cvm_preprocessing_scoring,
    create_cvm_preprocessing,
)
from nba.backtesting.backtesting_pipeline import create_nba_backtesting_pipeline
# from nba.gender_age_imputation.gender_age_imputation_pipeline import create_nba_gender_age_imputation_pipeline
from nba.gender_age_imputation.gender_age_imputation_pipeline import create_nba_gender_age_imputation_pipeline
from nba.model_input.model_input_pipeline import create_nba_model_input_pipeline
from nba.models.models_pipeline import create_nba_models_pipeline
from nba.pcm_scoring.pcm_scoring_pipeline import create_nba_pcm_scoring_pipeline
from nba.personnas_clustering.personnas_clustering_pipeline import create_nba_personnas_clustering_pipeline
from nba.report.pipelines.campaign_importance_volume_pipeline import (
    campaign_importance_volume,
)
from nba.report.pipelines.report_pipeline import create_use_case_view_report_pipeline
from nba.reporting.pipelines.nba_report_pipeline import create_gcg_marketing_performance_report_pipeline
from du.model_input.model_input_pipeline import(create_du_model_input_pipeline,
)
from du.models.models_pipeline import(create_du_models_pipeline,
)
from du.reporting.du_report_pipeline import(create_du_weekly_revenue_uplift_report_pipeline,)

from du.experiment.group_manage_pipeline import(create_du_test_group_pipeline,
)
from du.scoring.scoring_pipeline import(
create_du_scoring_pipeline,
create_package_preference_pipeline
)
from du.upsell.upsell_pipeline import(
create_du_upsell_pipeline,
create_du_weekly_low_score_pipeline,
)
from .pipelines.data_engineering.pipelines.campaign_pipeline import (
    campaign_to_l1_pipeline,
    campaign_to_l2_pipeline,
    campaign_to_l3_pipeline,
    campaign_to_l4_pipeline,
    campaign_to_l4_ranking_pipeline
)

from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l1.to_l1_pipeline import (
    complaints_to_l1_pipeline,
    complaints_to_l1_pipeline_survey,

)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l2.to_l2_pipeline import (
    complaints_to_l2_pipeline,
)
from .pipelines.data_engineering.pipelines.complaints_pipeline.to_l3.to_l3_pipeline import (
    complaints_to_l3_pipeline,
    complaints_to_l3_pipeline_survey
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
    digital_to_l1_pipeline,
    digital_to_l2_pipeline,
    digital_to_l3_pipeline,
    digital_to_l4_monthly_pipeline,
    digital_to_l4_weekly_pipeline,
    digital_to_l4_weekly_favourite_pipeline,
)

# add new pipeline import
from .pipelines.data_engineering.pipelines.digital_pipeline.to_l1.to_l1_pipeline import (
    digital_to_l1_app_agg_daily_pipeline,
    digital_to_l1_aib_categoy_clean_master,
    digital_to_l1_digital_mobile_web_agg_daily,
    digital_to_l1_app_agg_timeband_pipeline,
    digital_to_l1_digital_mobile_web_agg_timeband,
    digital_to_l1_customer_relay_agg_daily,
    digital_to_l1_combine_app_web_daily,
    digital_to_l1_cxense_traffic_daily_agg_pipeline,
    digital_to_l1_cxense_content_profile,
    digital_to_l1_cxense_traffic_daily_agg_non_site_id_pipeline,
    digital_to_l1_digital_mobile_combine_agg_timeband
)

from .pipelines.data_engineering.pipelines.digital_pipeline.to_l3.to_l3_pipeline import (
    digital_to_l3_digital_mobile_web_agg_monthly,
    digital_to_l3_customer_relay_agg_monthly,
    digital_to_l3_app_monthly_feature_pipeline,
    digital_to_l3_digital_combine_feature_monthly,
    digital_to_l3_app_monthly_feature_favorite,
    digital_to_l3_web_monthly_feature_favorite,
    digital_to_l3_app_agg_timeband_monthly_pipeline,
    digital_to_l3_digital_mobile_web_agg_timeband,
    digital_to_l3_combine_monthly_feature_favorite,
    digital_to_l3_app_favorite_timeband_pipeline,
    digital_to_l3_web_favorite_timeband_pipeline,
    digital_to_l3_app_monthly_feature_score,
    digital_to_l3_web_monthly_feature_score,
    digital_to_l3_combine_monthly_feature_score,
    digital_to_l3_combine_favorite_by_category_monthly,
    digital_to_l3_combine_category_timeband_monthly,
    digital_to_l3_cxense_agg_monthly
)

from .pipelines.data_engineering.pipelines.digital_pipeline.to_l4.to_l4_pipeline import (
    digital_to_l4_digital_customer_app_category_monthly,
    digital_to_l4_digital_customer_combine_agg_monthly,
    digital_to_l4_digital_customer_web_category_monthly,
    digital_to_l4_digital_customer_web_category_timeband_monthly,
    digital_to_l4_customer_relay_monthly,
    digital_to_l4_digital_customer_app_category_timeband_monthly,
    digital_to_l4_digital_customer_combine_category_timeband_monthly,
)

from .pipelines.data_engineering.pipelines.loyalty_pipeline import (
    loyalty_to_l1_pipeline,
    loyalty_to_l2_pipeline,
    loyalty_to_l3_pipeline,
    loyalty_to_l4_weekly_pipeline,
    loyalty_to_l4_monthly_pipeline,
)
from .pipelines.data_engineering.pipelines.network_pipeline.to_l1.to_l1_pipeline import (
    network_to_l1_pipeline, 
    network_geo_home_work_location_master_to_l1_pipeline,
    network_cei_voice_qoe_to_l1_pipeline,
    network_data_traffic_to_l1_pipeline,
    network_failed_call_attempt_and_call_drop_to_l1_pipeline,
    network_cqi_to_l1_pipeline,
    network_cei_to_l1_pipeline
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
    revenue_feature_to_l3_pipeline,
    revenue_to_l4_monthly_pipeline,
    revenue_to_l2_pipeline,
    revenue_to_l4_weekly_pipeline,
)
from .pipelines.data_engineering.pipelines.revenue_pipeline.to_l3.to_l3_pipeline import revenue_l3_last_most_monthly
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l1.to_l1_pipeline import (
    streaming_to_l1_onair_vimmi_pipeline, streaming_to_l1_soc_mobile_data_pipeline,
    streaming_to_l1_session_duration_pipeline, streaming_sdr_sub_app_hourly_daily_for_l3_monthly
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l2.to_l2_pipeline import (
    streaming_to_l2_intermediate_pipeline, streaming_to_l2_pipeline, streaming_to_l2_session_duration_pipeline
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l3.to_l3_pipeline import (
    streaming_to_l3_pipeline, streaming_to_l3_session_duration_pipeline, streaming_series_title_master,
    streaming_to_l3_sdr_sub_app_time_based_features, streaming_to_l3_favourite_location_features,
)
from .pipelines.data_engineering.pipelines.stream_pipeline.to_l4.to_l4_pipeline import (
    streaming_l2_to_l4_pipeline,
    streaming_l2_to_l4_session_duration_pipeline,
    streaming_l1_to_l4_pipeline,
)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l1.to_l1_pipeline import (
    touchpoints_to_l1_pipeline,touchpoints_to_l1_pipeline_contact,

)
from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l2.to_l2_pipeline import (
    touchpoints_to_l2_pipeline,
)

from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l3.to_l3_pipeline import (
    touchpoints_to_l3_pipeline,
    touchpoints_to_l3_pipeline_contact,
)

from .pipelines.data_engineering.pipelines.touchpoints_pipeline.to_l4.to_l4_pipeline import (
    touchpoints_to_l4_pipeline,
)

from .pipelines.data_engineering.pipelines.usage_pipeline import (
    usage_to_l1_pipeline,
    usage_create_master_data_for_favourite_feature,
    usage_to_l1_pipeline_last_idd_features,
    usage_to_l2_pipeline,
    usage_to_l3_pipeline,
    usage_to_l3_pipeline_idd_features,
    usage_to_l4_pipeline,
    usage_to_l4_daily_pipeline,
)
from data_quality.pipeline import (
     data_quality_pipeline,
     subscription_id_sampling_pipeline,
     threshold_analysis_pipeline
)

from .pipelines.data_engineering.pipelines.sales_pipeline.to_l2.to_l2_pipeline import (
    sales_to_l2_pipeline,
)

from .pipelines.data_engineering.pipelines.sales_pipeline.to_l4.to_l4_weekly_pipeline import (
    sales_to_l4_pipeline,
)

from .pipelines.data_engineering.pipelines.util_pipeline import (
    lineage_dependency_pipeline, ops_report_pipeline, metadata_backup_pipeline
)

from .pipelines.data_engineering.pipelines.predictive_pipeline.to_l4.to_l4_pipeline import predictive_to_l4_pipeline

from .pipelines.data_engineering.pipelines.geolocation_pipeline.to_l1.to_l1_pipeline import *
from .pipelines.data_engineering.pipelines.geolocation_pipeline.to_l2.to_l2_pipeline import *
from .pipelines.data_engineering.pipelines.geolocation_pipeline.to_l3.to_l3_pipeline import *
from .pipelines.data_engineering.pipelines.geolocation_pipeline.to_l4.to_l4_pipeline import *



def create_c360_pipeline(**kwargs) -> Dict[str, Pipeline]:

    return {
        "usage_to_l1_pipeline": usage_to_l1_pipeline(),
        "usage_create_master_data_for_favourite_feature": usage_create_master_data_for_favourite_feature(),
        "usage_to_l1_pipeline_last_idd_features": usage_to_l1_pipeline_last_idd_features(),
        "usage_to_l4_daily_pipeline": usage_to_l4_daily_pipeline(),
        "usage_to_l2_pipeline": usage_to_l2_pipeline(),
        "usage_to_l3_pipeline": usage_to_l3_pipeline(),
        "usage_to_l3_pipeline_idd_features": usage_to_l3_pipeline_idd_features(),
        "usage_to_l4_pipeline": usage_to_l4_pipeline(),
        "customer_profile_to_l1_pipeline": customer_profile_to_l1_pipeline(),
        "customer_profile_to_l2_pipeline": customer_profile_to_l2_pipeline(),
        "customer_profile_to_l3_pipeline": customer_profile_to_l3_pipeline(),
        "unioned_customer_profile_to_l3_pipeline": unioned_customer_profile_to_l3_pipeline(),
        "customer_profile_billing_level_to_l3_pipeline": customer_profile_billing_level_to_l3_pipeline(),
        "billing_to_l1_pipeline": billing_to_l1_pipeline(),
        "billing_l0_to_l3_pipeline": billing_l0_to_l3_pipeline(),
        "billing_l1_to_l3_pipeline": billing_l1_to_l3_pipeline(),
        "billing_to_l2_pipeline": billing_to_l2_pipeline(),
        "billing_to_l4_pipeline_monthly": billing_to_l4_pipeline_monthly(),
        "billing_to_l4_pipeline_weekly": billing_to_l4_pipeline_weekly(),
        "billing_to_l4_ranked_pipeline_weekly": billing_to_l4_ranked_pipeline_weekly(),
        "billing_to_l4_pipeline_daily": billing_to_l4_pipeline_daily(),
        "device_to_l1_pipeline": device_to_l1_pipeline(),
        "device_to_l2_pipeline": device_to_l2_pipeline(),
        "device_to_l4_pipeline": device_to_l4_pipeline(),
        "digital_to_l1_pipeline": digital_to_l1_pipeline(),
        "digital_to_l3_app_monthly_feature_pipeline": digital_to_l3_app_monthly_feature_pipeline(),
        "digital_to_l3_combine_monthly_feature_favorite": digital_to_l3_combine_monthly_feature_favorite(),
        "digital_to_l3_cxense_agg_monthly": digital_to_l3_cxense_agg_monthly(),
        # add
        "digital_to_l1_app_agg_daily_pipeline": digital_to_l1_app_agg_daily_pipeline(),
        "digital_to_l1_aib_categoy_clean_master" : digital_to_l1_aib_categoy_clean_master(),
        "digital_to_l1_digital_mobile_web_agg_daily" : digital_to_l1_digital_mobile_web_agg_daily(),
        "digital_to_l1_app_agg_timeband_pipeline"   :   digital_to_l1_app_agg_timeband_pipeline(),
        "digital_to_l1_customer_relay_agg_daily": digital_to_l1_customer_relay_agg_daily(),
        "digital_to_l1_digital_mobile_web_agg_timeband" : digital_to_l1_digital_mobile_web_agg_timeband(),
        "digital_to_l1_cxense_traffic_daily_agg_pipeline" :  digital_to_l1_cxense_traffic_daily_agg_pipeline(),
        "digital_to_l1_cxense_content_profile" : digital_to_l1_cxense_content_profile(),
        "digital_to_l1_cxense_traffic_daily_agg_non_site_id_pipeline" : digital_to_l1_cxense_traffic_daily_agg_non_site_id_pipeline(),
        "digital_to_l3_digital_mobile_web_agg_monthly": digital_to_l3_digital_mobile_web_agg_monthly(),
        "digital_to_l3_customer_relay_agg_monthly": digital_to_l3_customer_relay_agg_monthly(),
        "digital_to_l1_combine_app_web_daily" : digital_to_l1_combine_app_web_daily(),
        "digital_to_l3_digital_combine_feature_monthly" : digital_to_l3_digital_combine_feature_monthly(),
        "digital_to_l3_app_monthly_feature_favorite" :digital_to_l3_app_monthly_feature_favorite(),
        "digital_to_l3_web_monthly_feature_favorite" : digital_to_l3_web_monthly_feature_favorite(),
        "digital_to_l3_app_agg_timeband_monthly_pipeline" :digital_to_l3_app_agg_timeband_monthly_pipeline(),
        "digital_to_l3_digital_mobile_web_agg_timeband":digital_to_l3_digital_mobile_web_agg_timeband(),
        "digital_to_l3_app_favorite_timeband_pipeline" : digital_to_l3_app_favorite_timeband_pipeline(),
        "digital_to_l3_web_favorite_timeband_pipeline":digital_to_l3_web_favorite_timeband_pipeline(),
        "digital_to_l3_app_monthly_feature_score" : digital_to_l3_app_monthly_feature_score(),
        "digital_to_l3_web_monthly_feature_score" : digital_to_l3_web_monthly_feature_score(),
        "digital_to_l3_combine_monthly_feature_score" : digital_to_l3_combine_monthly_feature_score(),
        "digital_to_l4_digital_customer_app_category_monthly" : digital_to_l4_digital_customer_app_category_monthly(),
        "digital_to_l4_digital_customer_web_category_monthly" : digital_to_l4_digital_customer_web_category_monthly(),
        "digital_to_l3_combine_favorite_by_category_monthly": digital_to_l3_combine_favorite_by_category_monthly(),
        "digital_to_l4_digital_customer_combine_agg_monthly": digital_to_l4_digital_customer_combine_agg_monthly(),
        "digital_to_l4_digital_customer_web_category_timeband_monthly": digital_to_l4_digital_customer_web_category_timeband_monthly(),
        "digital_to_l1_digital_mobile_combine_agg_timeband" : digital_to_l1_digital_mobile_combine_agg_timeband(),
        "digital_to_l4_customer_relay_monthly": digital_to_l4_customer_relay_monthly(),
        "digital_to_l3_combine_category_timeband_monthly" : digital_to_l3_combine_category_timeband_monthly(),
        "digital_to_l4_digital_customer_app_category_timeband_monthly": digital_to_l4_digital_customer_app_category_timeband_monthly(),
        "digital_to_l4_digital_customer_combine_category_timeband_monthly" : digital_to_l4_digital_customer_combine_category_timeband_monthly(),

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
        "streaming_series_title_master": streaming_series_title_master(),
        "streaming_to_l3_pipeline": streaming_to_l3_pipeline(),
        "streaming_sdr_sub_app_hourly_daily_for_l3_monthly": streaming_sdr_sub_app_hourly_daily_for_l3_monthly(),
        "streaming_to_l3_sdr_sub_app_time_based_features": streaming_to_l3_sdr_sub_app_time_based_features(),
        "streaming_to_l3_favourite_location_features": streaming_to_l3_favourite_location_features(),
        "streaming_to_l3_session_duration_pipeline": streaming_to_l3_session_duration_pipeline(),
        "streaming_l1_to_l4_pipeline": streaming_l1_to_l4_pipeline(),
        "streaming_l2_to_l4_session_duration_pipeline": streaming_l2_to_l4_session_duration_pipeline(),
        "streaming_l2_to_l4_pipeline": streaming_l2_to_l4_pipeline(),
        "revenue_to_l1_pipeline": revenue_to_l1_pipeline(),
        "revenue_to_l2_pipeline": revenue_to_l2_pipeline(),
        "revenue_to_l3_pipeline": revenue_to_l3_pipeline(),
        "revenue_feature_to_l3_pipeline": revenue_feature_to_l3_pipeline(),
        "revenue_to_l4_daily_pipeline": revenue_to_l4_daily_pipeline(),
        "revenue_to_l4_monthly_pipeline": revenue_to_l4_monthly_pipeline(),
        "revenue_to_l4_weekly_pipeline": revenue_to_l4_weekly_pipeline(),
        "revenue_l3_last_most_monthly": revenue_l3_last_most_monthly(),
        "complaints_to_l1_pipeline": complaints_to_l1_pipeline(),
        "complaints_to_l1_pipeline_survey": complaints_to_l1_pipeline_survey(),
        "complaints_to_l2_pipeline": complaints_to_l2_pipeline(),
        "complaints_to_l3_pipeline": complaints_to_l3_pipeline(),
        "complaints_to_l3_pipeline_survey": complaints_to_l3_pipeline_survey(),
        "complaints_to_l4_pipeline": complaints_to_l4_pipeline(),
        "touchpoints_to_l1_pipeline": touchpoints_to_l1_pipeline(),
        "touchpoints_to_l1_pipeline_contact": touchpoints_to_l1_pipeline_contact(),
        "touchpoints_to_l2_pipeline": touchpoints_to_l2_pipeline(),
        "touchpoints_to_l3_pipeline": touchpoints_to_l3_pipeline(),
        "touchpoints_to_l3_pipeline_contact": touchpoints_to_l3_pipeline_contact(),
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
        "network_geo_home_work_location_master_to_l1_pipeline": network_geo_home_work_location_master_to_l1_pipeline(),
        "network_cei_voice_qoe_to_l1_pipeline": network_cei_voice_qoe_to_l1_pipeline(),
        "network_data_traffic_to_l1_pipeline": network_data_traffic_to_l1_pipeline(),
        "network_failed_call_attempt_and_call_drop_to_l1_pipeline": network_failed_call_attempt_and_call_drop_to_l1_pipeline(),
        "network_cqi_to_l1_pipeline": network_cqi_to_l1_pipeline(),
        "network_cei_to_l1_pipeline": network_cei_to_l1_pipeline(),
        "network_to_l2_pipeline": network_to_l2_pipeline(),
        "network_to_l3_pipeline": network_to_l3_pipeline(),
        "network_to_l4_pipeline": network_to_l4_pipeline(),
        "product_to_l1_pipeline": product_to_l1_pipeline(),
        "product_to_l2_pipeline": product_to_l2_pipeline(),
        "product_to_l4_pipeline": product_to_l4_pipeline(),
        "sales_to_l2_pipeline": sales_to_l2_pipeline(),
        "sales_to_l4_pipeline": sales_to_l4_pipeline(),
        "predictive_to_l4_pipeline": predictive_to_l4_pipeline(),
        "lineage_dependency_pipeline": lineage_dependency_pipeline(),
        "ops_report_pipeline": ops_report_pipeline(),
        "geo_to_l3_pipeline_1st": geo_to_l3_pipeline_1st(),
        "geo_to_l3_pipeline_2nd": geo_to_l3_pipeline_2nd(),
        "geo_to_l3_home_work_pipeline": geo_to_l3_home_work_pipeline(),
        "geo_to_l1_pipeline_master": geo_to_l1_pipeline_master(),
        "geo_to_l1_pipeline_to_run":geo_to_l1_pipeline_to_run(),
        "geo_to_l1_pipeline": geo_to_l1_pipeline(),
        "geo_to_l2_pipeline": geo_to_l2_pipeline(),
        "geo_to_l3_pipeline": geo_to_l3_pipeline(),
        "geo_to_l4_pipeline": geo_to_l4_pipeline(),
        "metadata_backup_pipeline": metadata_backup_pipeline()
    }


def create_cvm_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "cvm_setup_training_data_sample": create_cvm_prepare_inputs_samples("sample")
        + create_cvm_targets("sample")
        + create_cvm_training_data("sample"),
        "cvm_training_preprocess_sample": create_cvm_preprocessing_scoring("sample"),
        "cvm_train_model_sample": create_train_model("sample"),
        "cvm_setup_scoring_data_sample": create_cvm_prepare_inputs_samples(
            "scoring_sample"
        )
        + create_cvm_scoring_data("scoring_sample"),
        "cvm_scoring_combine_data": create_cvm_scoring_data("scoring_sample"),
        "cvm_scoring_preprocess_sample": create_cvm_preprocessing_scoring(
            "scoring_sample"
        ),
        "cvm_predict_model_sample": create_predictions("scoring_sample", "dev"),
        "cvm_setup_training_data_dev": create_cvm_prepare_inputs_samples("dev")
        + create_cvm_targets("dev")
        + create_cvm_training_data("dev"),
        "cvm_training_combine_data": create_cvm_training_data("dev"),
        "cvm_training_preprocess_dev": create_cvm_preprocessing("dev"),
        "cvm_train_model_dev": create_train_model("dev"),
        "cvm_setup_scoring_data_dev": create_cvm_prepare_inputs_samples("scoring_dev")
        + create_cvm_scoring_data("scoring_dev"),
        "cvm_scoring_preprocess_dev": create_cvm_preprocessing_scoring("scoring_dev"),
        "cvm_predict_model_dev": create_predictions("scoring_dev", "dev"),
        "cvm_validate_model_dev": create_predictions(
            "sample", "dev", "l5_cvm_one_day_train_preprocessed_sample"
        ),
    }


def create_nba_pipeline(**kwargs) -> Dict[str, Pipeline]:
    return {
        "__default__": create_nba_model_input_pipeline()
        + create_nba_models_pipeline()
        + campaign_importance_volume()
        + create_nba_backtesting_pipeline()
        + create_nba_pcm_scoring_pipeline()
        + create_nba_gender_age_imputation_pipeline()
        + create_nba_personnas_clustering_pipeline(),
        "create_use_case_view_report": create_use_case_view_report_pipeline(),
        "create_gcg_marketing_performance_report": create_gcg_marketing_performance_report_pipeline(),
    }

def create_du_pipeline(**kwargs) -> Dict[str,Pipeline]:
    return {
        "create_du_model_input": create_du_model_input_pipeline(),
        "create_du_model": create_du_models_pipeline(),
        "create_du_test_group": create_du_test_group_pipeline(),
        "create_du_scoring": create_du_scoring_pipeline(),
        "create_du_upsell": create_du_upsell_pipeline(),
        "create_package_preference": create_package_preference_pipeline(),
        "create_du_weekly_revenue_uplift_report":create_du_weekly_revenue_uplift_report_pipeline(),
        "create_du_weekly_low_score_list":create_du_weekly_low_score_pipeline(),
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
        create_du_pipeline(**kwargs).items()
    ):
        # If many pipelines have nodes under the same modular
        # pipeline, combine the results
        if pipeline_name in all_pipelines.keys():
            all_pipelines[pipeline_name] += pipeline_object
        else:
            all_pipelines[pipeline_name] = pipeline_object

    return all_pipelines
