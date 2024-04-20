import pandas as pd
import os

def exportLog(id, export_file_name, scrape_time, clean_time, status):
    file_path = "data/log/" + export_file_name
    if not os.path.exists(file_path):
        df = pd.DataFrame(columns=['id', 'scrape_time', 'clean_time', 'status'])
        df.to_csv(file_path, index=False)

    df = pd.read_csv(file_path, header=0)
    # append the new row to the dataframe
    if id in df['id'].values and df.loc[df['id'] == id, 'status'].values[0] == "fail": 
        # here 'status' is the column name returned by the query
        # the second parameter from loc[] is the column name, could a single column or multiple columns (in a form of list)
        # update the status to status
        df.loc[df['id'] == id, 'status'] = status
    else:
        df.loc[df.shape[0]] = [id, scrape_time, clean_time, status]
        df = df.sort_values(by='id', ascending=True)
    
    df.to_csv(file_path, index=False)

# exportLog(1, "log_cnn.csv", None, None, "fail")