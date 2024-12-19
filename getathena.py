import boto3
import sys
import json
import pandas as pd

'''
select
dt, version_id,
audience_id,
visit_start_datetime, event_start_datetime_ms,
playback_time, first_level_themes								
from audience_activity 
where version_id in('p0gh5q9m','p0gh6cp1', 'p0gh7y12', 'p0ghw0r1', 'p0gh684p', 'p0gh4n67') and dt = '20231124'
'''
session = boto3.Session(profile_name='aud')
athena = session.client('athena')
paginator = athena.get_paginator('get_query_results')

def start_query(date):
    query = f"select version_id, audience_id, event_start_datetime_ms, playback_time from audience_activity where first_level_themes like '%video_pop_up_channel_%' and version_id not like 'video_%' and dt = '{date}'"
    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup='dazzler',
        QueryExecutionContext={'Database':'audience'}
    )
    return response["QueryExecutionId"]

def get_data(date):
    qeid = start_query(date)
    response = athena.get_query_execution(QueryExecutionId = qeid)
    while response['QueryExecution']['Status']['State'] in ['QUEUED', 'RUNNING']:
        response = athena.get_query_execution(QueryExecutionId = qeid)
    response_iterator = paginator.paginate(QueryExecutionId=qeid)
    for page in response_iterator:
        for row in page['ResultSet']['Rows']:
            r = [f.get('VarCharValue', '') for f in row['Data']]
            if r[3].isdigit():
                yield r

if __name__ == '__main__':
    date = sys.argv[1]
    l = [r for r in get_data(date)]
    # l = [r for r in l if r[3].isdigit()]
    # print(json.dumps(l))
    df = pd.DataFrame(l, columns=['channel_pid', 'audience_id', 'event_start_datetime_ms', 'playback_time'])
    # df['event_start_datetime_ms'] = df['event_start_datetime_ms'].astype('timestamp64[ns]')
    df['playback_time'] = df['playback_time'].astype(int)
    print(df.info())
    df.to_csv(f'{date}.csv', index=False)

