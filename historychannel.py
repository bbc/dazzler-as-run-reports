import pandas as pd
from datetime import datetime, timedelta
from piano_data import getSheet
from data_from_nitro import resolve
from ml_asruns import get_as_run

channel = 'History_Channel'
channelId = '7510516'
region = 'eu-west-2'
account = '739777694307'

# ml = boto3.client('medialive', region_name='eu-west-2')
# mlArn = [c for c in ml.list_channels()['Channels'] if channel in c['Name']][0]['Arn']

def get_day(day):
    t = datetime.fromisoformat(f'{day}T00:00:00+01:00')
    s = t - timedelta(hours=4)
    e = t + timedelta(hours=24)
    report = get_as_run(channelId, account, region, s.isoformat(), e.isoformat())
    print('got', len(report), 'as_run records')
    nitrodata = {vpid: resolve(vpid) for vpid in set([r['vpid'] for r in report])}  
    print('got', len(nitrodata), 'unique vpids')
    for row in report:
        entity = nitrodata[row['vpid']]
        row['type'] = entity['item_type']
        row['pid'] = entity['pid']
        row['title'] = entity['title']
    return report

def merge(day, sched, stats):
    sched = pd.DataFrame.from_records(sched, 'timestamp').convert_dtypes()
    sched.index = pd.to_datetime(sched.index)

    e = (datetime.strptime(day, '%Y-%m-%d')+timedelta(hours=24)).strftime('%F')
    asrun = sched.reindex(pd.date_range(day, e, freq='T', tz='Europe/London'), method='pad')

    m = asrun.reset_index()

    stats = m.merge(stats.reset_index(), left_index=True, right_index=True)
    stats.index = stats['index_x']
    stats.index = stats.index.tz_localize(None)
    return stats.drop(columns=['index_x','index_y', 'duration', 'start', 'delta', 'direction'])

viewers = getSheet('iPlayer Streams Users - History Channel.xlsx')
r=pd.date_range('2023-08-09','2023-08-16')
with pd.ExcelWriter("history.xlsx") as writer:
    for d in r:
        day = d.isoformat().split('T')[0]
        x = f'{day} 00:00:00.1'
        stats=viewers[x][1:]
        sched = get_day(day)
        sheet = merge(day, sched, stats)
        sheet.to_excel(writer, sheet_name=day, index=True)
