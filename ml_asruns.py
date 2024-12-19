import json
from datetime import datetime
import boto3
import time
import sys
import pandas as pd

def unixtime(dt):
  return 1000*int(time.mktime(dt.timetuple()))

def get_as_run(region, lsn, start, end):
    sdt = datetime.fromisoformat(start)
    edt = datetime.fromisoformat(end)
    client = boto3.client('logs', region_name=region)
    paginator = client.get_paginator('filter_log_events')
    response_iterator = paginator.paginate(
        logGroupName='ElementalMediaLive',
        logStreamNames=[lsn],
        startTime=unixtime(sdt),
        endTime=unixtime(edt),
        filterPattern='"input_switch_initiated"',
        PaginationConfig={
            'MaxItems': 10000,
            'PageSize': 100,
            # 'StartingToken': '',
        }
    )
    return response_iterator

def gen(response_iterator):
    for page in response_iterator:
        for line in page['events']:
            yield json.loads(line['message'])

def cmap(c):
    return {
        'name': c['Name'],
        'arn': c['Arn'],
        **c['Tags'],
    }

def get_simple_as_run_report(region, start, end):
    ml = boto3.client('medialive', region_name=region)
    channels = [cmap(c) for c in ml.list_channels()['Channels']]
    for channel in channels:
        arn = channel['arn']
        bits = arn.split(':')
        lsn = f'arn_aws_medialive_{bits[3]}_{bits[4]}_channel_{bits[6]}_0'
        if 'vpid' in channel:
            iplayervpid = channel['vpid']
            name = channel['name'].split(' ')[1]
        else:
            print('ignoring', channel['name'])
            continue
        print(iplayervpid)
        response_iterator = get_as_run(region, lsn, start, end)
        for line in gen(response_iterator):
            if 'channel_arn' in line:
                arn = line['channel_arn']
                message = json.loads(line['message'])
                action = message['action_name']
                ts = line['timestamp']
                planned, duration, source, vpid = action.split(' ')
                yield {'name': name, 'start': ts, 'duration': duration, 'channel_vpid': iplayervpid, 'item_vpid': vpid}

if __name__ == '__main__':
    start = sys.argv[1]
    end = sys.argv[2]
    irl = pd.DataFrame(get_simple_as_run_report('eu-west-1', start, end))
    lon = pd.DataFrame(get_simple_as_run_report('eu-west-2', start, end))
    df = pd.concat([irl,lon])
    df2=df.astype({'name': 'string', 'start': 'datetime64[ns]', 'duration': 'timedelta64[ns]', 'channel_vpid': 'string', 
'item_vpid': 'string'})
    df2['end'] = df2['start'] + df2['duration']
    df2.to_parquet('p.pq')
    df3=pd.read_parquet('p.pq')
    print(df3.info())
