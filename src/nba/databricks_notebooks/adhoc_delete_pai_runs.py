import pai

TAGS_TO_DELETE = ["z_" + s + "_" for s in [
"20200403_010102_Miguel"
]]

pai.set_config(storage_runs="/dbfs/mnt/customer360-blob-data/NBA/pai")

for tag in TAGS_TO_DELETE:
    print(f"Deleting tag {tag}...")
    runs = pai.load_runs(tags = [tag])
    for _,run_info in runs.iterrows():
        pai.delete_run(run_id=run_info["run_id"])
        print(f"    Deleted run {run_info['run_name']}...")
