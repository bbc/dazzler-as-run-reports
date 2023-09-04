import json
import os
from datetime import datetime, timedelta, timezone
from isoduration import parse_duration, format_duration
import requests
import csv
import boto3
import time
import pandas as pd

apiKey = os.environ['API_KEY']

client = boto3.client('logs', region_name='eu-west-2')

episodes = {}
clips = {}

def round_seconds(obj: datetime) -> datetime:
    if obj.microsecond >= 500_000:
        obj += timedelta(seconds=1)
    return obj.replace(microsecond=0)

def resolve(vpid):
    r = requests.get(f'http://programmes.api.bbc.com/nitro/api/programmes?api_key={apiKey}&version={vpid}', headers={'Accept': 'application/json'})
    a = r.json()
    return a['nitro']['results']['items'][0]


# fields @timestamp, @message
# | sort @timestamp asc
# | limit 9999 
# | filter @message like /"as_run_type": "input_switch_initiated.*url_path/

def unixtime(dt):
  return 1000*int(time.mktime(datetime.fromisoformat(dt).timetuple()))

channel = 'History_Channel'
channelId = '7510516'
region = 'eu-west-2'
account = '739777694307'
viewers = pd.read_excel('iPlayer Streams Users - History Channel.xlsx')
k = viewers.keys()
days = viewers[k[2]]
print(days)

ml = boto3.client('medialive', region_name='eu-west-2')

mlArn = [c for c in ml.list_channels()['Channels'] if channel in c['Name']][0]['Arn']
lsn = f"arn_aws_medialive_{region}_{account}_channel_{channelId}_0_as_run"
print(lsn)

paginator = client.get_paginator('filter_log_events')

response_iterator = paginator.paginate(
    logGroupName='ElementalMediaLive',
    logStreamNames=[lsn],
    startTime=unixtime('2023-08-09T00:00:00Z'),
    endTime=unixtime('2023-08-10T00:00:00Z'),
    filterPattern='"input_switch_initiated"',
    PaginationConfig={
        'MaxItems': 10000,
        'PageSize': 100,
        # 'StartingToken': '',
    }
)

columns = ['type', 'start', 'duration', 'vpid', 'pid', 'title', 'direction', 'delta']
report = []
for page in response_iterator:
  for i in page['events']:
    row = {}
    timestamp = i["timestamp"]
    m = json.loads(i['message'])
    if m['as_run_type'] != 'input_switch_initiated':
        continue
    actionName = m["action_name"]
    path = m.get("url_path", '')
    if 'distributionbucket' in path:
        e = path.split('_')
        vpid = e[3]
    elif 'mp4' in path:
        vpid = path.split('/')[-1].split('.')[0]
    else:
        vpid = ''
    an = actionName.split(' ')
    if len(an) == 4:
        start, duration, source, pid = an
    else:
        start, duration, source = an
        pid = ''
    start = f"{start[0:4]}-{start[4:6]}-{start[6:8]}T{start[9:11]}:{start[11:13]}:{start[13:15]}.{start[16:19]}Z"
    planned = datetime.fromisoformat(start)
    actual = datetime.fromtimestamp(int(timestamp/1000), tz=timezone.utc)
    pdur = parse_duration(duration)
    row['vpid'] = vpid
    row['duration'] = format_duration(pdur)
    row['start'] = round_seconds(actual).strftime('%H:%M:%S')
    row['end'] = round_seconds(actual+pdur).strftime('%H:%M:%S')
    delta = (actual - planned).total_seconds()
    if delta > 0.5:
        row['direction'] = 'late'
        row['delta'] = delta
    elif delta < -0.5:
        direction = 'early'
        row['delta'] = -delta
    if pid != row.get('vpid', ''):
        print('?', pid, row.get('vpid', ''))
    entity = resolve(pid)
    row['type'] = entity['item_type']
    row['pid'] = entity['pid']
    row['title'] = entity['title']
    report.append(row)

    sched = pd.DataFrame(report)
    print(sched)
    print(sched.keys())