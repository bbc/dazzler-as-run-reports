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

CLIENT = boto3.client("athena", region_name='eu-west-1')
RESULT_OUTPUT_LOCATION = "s3://aws-athena-query-results-newssystemintegration/" #/Unsaved/2023/11/27/"

def start_query(PID, DATE):
    query = f"select dt, version_id, audience_id, visit_start_datetime, event_start_datetime_ms, playback_time, first_level_themes from audience_activity where version_id = '{PID}' and dt = '{DATE}'"
    response = CLIENT.start_query_execution(
        QueryString=query,
        #ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION},
        WorkGroup='dazzler'
    )

    return response["QueryExecutionId"]

def get_data(pid, date):
    dt = date.replace('-', '')
    pass

print(get_data('p0gh5q9m', '20231125'))

