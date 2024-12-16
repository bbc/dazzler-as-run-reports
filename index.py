import json
from datetime import datetime, timedelta
import boto3
import time
import sys
import io
import pandas as pd

s3_client = boto3.client('s3')

def unixtime(dt):
  return 1000*int(time.mktime(dt.timetuple()))

def get_as_run(region, lsn, start, end):
    client = boto3.client('logs', region_name=region)
    paginator = client.get_paginator('filter_log_events')
    response_iterator = paginator.paginate(
        logGroupName='ElementalMediaLive',
        logStreamNames=[lsn],
        startTime=unixtime(start),
        endTime=unixtime(end),
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
        response_iterator = get_as_run(region, lsn, start, end)
        for line in gen(response_iterator):
            if 'channel_arn' in line:
                arn = line['channel_arn']
                message = json.loads(line['message'])
                action = message['action_name']
                ts = line['timestamp']
                planned, duration, source, vpid = action.split(' ')
                yield {'name': name, 'start': ts, 'duration': duration, 'channel_vpid': iplayervpid, 'item_vpid': vpid}

def lambda_handler(event, context):
    time = event["time"]
    dt = datetime.fromisoformat(time)
    start = datetime.combine(dt.date(), datetime.min.time())
    end = start + timedelta(hours=24)
    region = event['region']
    print(region, start, end)
    df = pd.DataFrame(get_simple_as_run_report(region, start, end))
    df2=df.astype({'name': 'string', 'start': 'datetime64[ns]', 'duration': 'timedelta64[ns]', 'channel_vpid': 'string', 
'item_vpid': 'string'})
    df2['end'] = df2['start'] + df2['duration']
    out_buffer = io.BytesIO()
    df2.to_parquet(out_buffer, index=False, compression='gzip')
    s3_client.put_object(Bucket='iplayer-dazzler-asruns', Key=f'daily/{region}_{start.date().isoformat()}.gz', Body=out_buffer.getvalue())
    return {
        'statusCode': 200,
        'body': json.dumps('wrote daily as runs')
    }
