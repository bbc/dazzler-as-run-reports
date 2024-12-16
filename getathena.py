import boto3

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

def start_query(pid, date):
    query = f"select dt, version_id, audience_id, visit_start_datetime, event_start_datetime_ms, playback_time, first_level_themes from audience_activity where version_id = '{pid}' and dt = '{date}'"
    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup='dazzler',
        QueryExecutionContext={'Database':'audience'}
    )
    return response["QueryExecutionId"]

def get_data(pid, date):
    qeid = start_query(pid, date)
    response_iterator = paginator.paginate(QueryExecutionId=qeid)
    for page in response_iterator:
        print(page)