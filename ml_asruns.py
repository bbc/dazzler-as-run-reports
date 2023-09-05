import json
from datetime import datetime, timedelta, timezone
from isoduration import parse_duration, format_duration
import boto3
import time

def round_seconds(obj: datetime) -> datetime:
    if obj.microsecond >= 500_000:
        obj += timedelta(seconds=1)
    return obj.replace(microsecond=0)

def unixtime(dt):
  return 1000*int(time.mktime(dt.timetuple()))

def parseActionName(actionName):
    an = actionName.split(' ')
    s = an[0]
    start_dt = f"{s[0:4]}-{s[4:6]}-{s[6:8]}T{s[9:11]}:{s[11:13]}:{s[13:15]}.{s[16:19]}Z"
    duration = parse_duration(an[1])
    return  datetime.fromisoformat(start_dt), duration

def get_as_run(channelId, account, region, start, end):
    sdt = datetime.fromisoformat(start)
    edt = datetime.fromisoformat(end)
    client = boto3.client('logs', region_name=region)
    lsn = f"arn_aws_medialive_{region}_{account}_channel_{channelId}_0_as_run"
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
            planned, duration = parseActionName(actionName)
            actual = datetime.fromtimestamp(int(timestamp/1000), tz=sdt.tzinfo)
            row['timestamp'] = actual.isoformat()
            row['vpid'] = vpid
            row['duration'] = format_duration(duration)
            row['start'] = round_seconds(actual).strftime('%H:%M:%S')
            row['end'] = round_seconds(actual+duration).strftime('%H:%M:%S')
            delta = (actual - planned).total_seconds()
            if delta > 0.5:
                row['direction'] = 'late'
                row['delta'] = delta
            elif delta < -0.5:
                row['direction'] = 'early'
                row['delta'] = -delta
            report.append(row)
    return report