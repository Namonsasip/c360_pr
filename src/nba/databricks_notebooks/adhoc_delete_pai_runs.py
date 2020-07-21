# This notebook allows to delete PAI runs by their tag
# This is useful to delete old PAI runs since NBA creates
# a ton of runs and having too many can impact PAI performance

import pai

TAGS_TO_DELETE = ["z_" + s + "_" for s in ["20200403_010102_Miguel"]]

pai.set_config(
    storage_runs="mssql+pyodbc://mck_ds_360:paisql#Apr2020@azdwh-serv-da.database.windows.net/pai-mssql-db?driver=ODBC+Driver+17+for+SQL+Server",
    storage_artifacts="dbfs://mnt/customer360-blob-data/NBA/pai_deleteme20200428/",
)

for tag in TAGS_TO_DELETE:
    print(f"Deleting tag {tag}...")
    runs = pai.load_runs(tags=[tag])
    for _, run_info in runs.iterrows():
        pai.delete_run(run_id=run_info["run_id"])
        print(f"    Deleted run {run_info['run_name']}...")
