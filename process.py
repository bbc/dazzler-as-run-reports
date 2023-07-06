import json
import sys
from datetime import datetime, timedelta
from isoduration import parse_duration, format_duration
import requests
import csv

def round_seconds(obj: datetime) -> datetime:
    if obj.microsecond >= 500_000:
        obj += timedelta(seconds=1)
    return obj.replace(microsecond=0)

def v2e(vpid):
    r = requests.get(f'https://localhost:8443/version/_doc/pid.{vpid}', verify=False)
    a = r.json()
    if a['found']:
        return a['_source']['pips']['version']['version_of']['link']['pid']
    return None

def episode(pid):
    r = requests.get(f'https://localhost:8443/episode/episode/pid.{pid}', verify=False)
    a = r.json()
    if a['found']:
        return a['_source']
    return None

# fields @timestamp, @message
# | sort @timestamp asc
# | limit 9999 
# | filter @message like /"as_run_type": "input_switch_initiated.*url_path/

inp = sys.argv[1]
ondate = sys.argv[2]

outf = inp.replace('.json', f'-{ondate}.csv')

f = open(inp)

data = [row for row in json.load(f) if row['@timestamp'].startswith(ondate)]

columns = ['start', 'duration', 'vpid', 'episode pid', 'title', 'direction', 'delta']
report = []
for i in data:
    # row = {c:'' for c in columns}
    row = {}
    m = i['@message']
    actionName = m["action_name"]
    timestamp = m["timestamp"]
    path = m.get("url_path", '')
    if 'distributionbucket' in path:
        e = path.split('_')
        row['vpid'] = e[3]
    elif 'mp4' in path:
        row['vpid'] = path.split('/')[-1].split('.')[0]
    else:
        row['vpid'] = ''
    an = actionName.split(' ')
    if len(an) == 4:
        start, duration, source, pid = an
    else:
        start, duration, source = an
        pid = ''
    start = f"{start[0:4]}-{start[4:6]}-{start[6:8]}T{start[9:11]}:{start[11:13]}:{start[13:15]}.{start[16:19]}Z"
    planned = datetime.fromisoformat(start)
    actual = datetime.fromisoformat(f"{timestamp}Z")
    row['duration'] = format_duration(parse_duration(duration))
    row['start'] = round_seconds(actual).strftime('%H:%M:%S')
    delta = (actual - planned).total_seconds()
    if delta > 0.5:
        row['direction'] = 'late'
        row['delta'] = delta
    elif delta < -0.5:
        direction = 'early'
        row['delta'] = -delta
    if pid != row.get('vpid', ''):
        print('?', pid, row.get('vpid', ''))
    epid = v2e(pid)
    if epid:
        row['episode pid'] = epid
        ep = episode(epid)
        if ep:
            row['title'] = ep['pips']['episode']['title']['$']
    report.append(row)
f.close()
with open(outf, 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, columns)
    dict_writer.writeheader()
    dict_writer.writerows(report)
