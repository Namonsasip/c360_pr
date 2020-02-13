import pandas as pd
import os


for each_folder in os.listdir(os.getcwd()):
    try:
        df = pd.read_parquet(each_folder)
        df.to_csv(os.path.join(os.getcwd(), '{}.csv'.format(each_folder)))
    except:
        pass
