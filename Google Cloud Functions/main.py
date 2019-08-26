from google.cloud import bigquery
import time
from datetime import datetime
import json
import requests
import pandas as pd

client = bigquery.Client('dataengineerproject-250819')


def get_max_timestamp():
    query = (
        "SELECT max(created_time) max_timestamp \
            FROM `dataengineerproject-250819.DataEngineerDataset.posts` "
    )
    query_job = client.query(query, location="US")
    max_timestamp = list(query_job)[0].max_timestamp
    return max_timestamp


def string_to_datetime(s):
    return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S+0000')


def json_to_df(json_data):
    df = pd.DataFrame.from_dict(json_data)
    df['created_time'] = df['created_time'].apply(string_to_datetime).astype('datetime64[ms]')
    df['inserted_time'] = datetime.now()
    df['inserted_time'] = df['inserted_time'].astype('datetime64[ms]')
    return df


def process_link(url, params):
    r = requests.get(url, params=params)
    data = r.content
    encoding = r.encoding
    return json.loads(data.decode(encoding))


def batch_insert(df):
    dataset_ref = client.dataset('DataEngineerDataset')
    table_ref = dataset_ref.table('posts')
    client.load_table_from_dataframe(df, table_ref).result()
    destination_table = client.get_table(table_ref)
    print("Number of all rows {}.".format(destination_table.num_rows))

    
def insert_words():
    query = (
        """insert `dataengineerproject-250819.DataEngineerDataset.words` 
            select wip.id, wip.word, wip.word_count, current_timestamp() insert_time 
            from `dataengineerproject-250819.DataEngineerDataset.words_in_post` wip
                left join `dataengineerproject-250819.DataEngineerDataset.words` w
                on wip.id = w.id
                where w.id is null"""
    )
    query_job = client.query(query, location="US")
    query_job.result()


def main(event, context):
    url = "https://graph.facebook.com/v4.0/104835507525670/posts"
    params = {
        'access_token': 'PUT_ACCESS_TOKEN_HERE',
        'format': 'json',
        'limit': '25'
    }
    max_timestamp = get_max_timestamp()
    if max_timestamp:
        params['since'] = max_timestamp.timestamp()
    df = pd.DataFrame()
    while 1:
        JSON = process_link(url, params)
        data = JSON['data']
        if not data:
            break
        df_tmp = json_to_df(data)
        df = pd.concat([df, df_tmp])
        url = JSON['paging']['next']
    if df.shape[0]:
        batch_insert(df.set_index('id', drop=True))
        insert_words()

