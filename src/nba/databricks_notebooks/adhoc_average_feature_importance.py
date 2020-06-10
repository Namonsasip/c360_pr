# This notebook computes the average feature importance accross all models from a given tag
# This is useful to see the average feature importance for all campaigns at once,
# which is not possible in PAI web UI

import pai
import pandas as pd

pd.set_option("max_colwidth", 100)

TAGS_TO_COMPARE = ["z_20200416_010819_acceptance_Miguel_"]

pai.set_config(
    storage_runs="mssql+pyodbc://mck_ds_360:paisql#Apr2020@azdwh-serv-da.database.windows.net/pai-mssql-db?driver=ODBC+Driver+17+for+SQL+Server",
    storage_artifacts="dbfs://mnt/customer360-blob-data/NBA/pai_deleteme20200428/",
)

runs_features = pai.load_features(tags=TAGS_TO_COMPARE)

# Filter only runs where a model was trained

runs_features = runs_features[
    runs_features["feature_list"].apply(lambda x: len(x) != 0)
]

n_runs = len(runs_features)

runs_features_melted = runs_features.melt(
    id_vars=[x for x in runs_features if not x.startswith("importance_")],
    value_vars=[x for x in runs_features if x.startswith("importance_")],
    var_name="feature",
    value_name="importance",
)

importance_summary = (
    runs_features_melted.groupby("feature", as_index=False)
    .agg({"importance": lambda x: x.sum() / n_runs})
    .sort_values("importance", ascending=False)
)

importance_summary
