import pandas as pd
import os

df = pd.read_csv("profile_drm_t_active_profile_customer_journey_monthly.csv")
df.to_parquet(os.path.join(os.getcwd(),
                           'mod_profile_drm_t_active_profile_customer_journey_monthly'),
              partition_cols=["partition_month"])
