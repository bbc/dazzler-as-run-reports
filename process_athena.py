import pandas as pd
import numpy as np
import sys
from datetime import datetime, timedelta
from ml_asruns import get_as_run
from dazzler_schedule import get_planned

config = {
    'p0gh5q9m': { 'sid': 'Arts_Channel'},
    'p0gh6cp1': { 'sid': 'History_Channel'},
    'p0gh7y12':  { 'sid': 'Louis_Theroux'},
    'p0ghw0r1':  { 'sid': 'Quiz_Channel'},
    'p0gh684p':  { 'sid': 'Real_Lives_Channel'},
    'p0gh4n67':  { 'sid': 'childrens_animation'},
}

bucket = 'childrens-dazzler'

def get_day(channelId, account, region, day):
    t = datetime.fromisoformat(f'{day}T00:00:00+01:00')
    s = t - timedelta(hours=4)
    e = t + timedelta(hours=24)
    return get_as_run(channelId, account, region, s.isoformat(), e.isoformat())

def watched(audience_row, schedule):
    s = audience_row['start']
    e = audience_row['end']
    startsWithin = schedule.loc[s:e]
    # starting before the session and ending within the session
    missedTheStart = schedule.loc[(schedule.index < s) & (schedule['e'] > s) & (schedule['e'] <= e)].copy()
    missedTheStart['seen'] = missedTheStart['e'] - s
    # starting and ending within the session
    sawItAll = startsWithin.loc[startsWithin['e'] <= e].copy()
    sawItAll['seen'] = sawItAll['d']
    # starting within the session and ending after
    missedTheEnd = startsWithin.loc[startsWithin['e'] > e].copy()
    missedTheEnd['seen'] = e - missedTheEnd.index
    # starting before the session and ending after the session
    sampledIt = schedule.loc[(schedule.index < s) & (schedule['e'] > e)].copy()
    sampledIt['seen'] = e - s
    wanted = pd.concat([missedTheStart, sawItAll, missedTheEnd, sampledIt])
    wanted['User ID'] = audience_row['audience_id']
    wanted['AV - Playback time'] = wanted['seen']  / np.timedelta64(1, 'ms')
    wanted['Item Start'] = wanted.index.tz_convert('Europe/London').tz_localize(None)
    wanted['Session Start'] = audience_row['start'].tz_localize(None)
    wanted['Session End'] = audience_row['end'].tz_localize(None)
    return wanted

def getscheduleforsessions(sessions, bucket, sid):
    first = sessions.index.tz_convert('UTC').min().date()
    last = sessions['end'].array.tz_convert('UTC').max().date()
    r = pd.date_range(first, last)
    s = []
    for d in r:
        day = d.date().isoformat()
        data = get_planned(bucket, sid, day)
        s = s + data
    sched = pd.DataFrame.from_records(s)
    sched.index = pd.to_datetime(sched['start'])
    sched['d'] = pd.to_timedelta(sched['duration'])
    sched['e'] = pd.to_datetime(sched['end'])
    return sched

# "dt","version_id","audience_id","visit_start_datetime","event_start_datetime_ms","playback_time","first_level_themes"

def getaudiencedata(filename):
    viewers = pd.read_csv(filename, index_col=4, parse_dates=True)
    viewers=viewers[viewers.playback_time>3]
    viewers.index = viewers.index.tz_localize('UTC')
    viewers['start'] = viewers.index
    viewers['end'] = viewers['start'] + pd.to_timedelta(viewers['playback_time'], unit='s')
    return viewers

def getaudiencedataJohn(filename):
    viewers = pd.read_csv(filename, header=0, index_col=2, dtype={'a': str, 'b': str, 'c': object, 'd': np.float64, 'e': np.int32, 'f': np.float64}, parse_dates=True)
    viewers.index = viewers.index.tz_localize('Europe/London')
    viewers['start'] = viewers.index
    viewers['end'] = viewers.index + pd.to_timedelta(viewers['AV - Playback time'], unit='ms')
    return viewers

def report_for_channel(viewers, bucket, sid):
    sched = getscheduleforsessions(viewers, bucket, sid)
    with pd.ExcelWriter(audience_data.replace('.csv', f'-{sid}-dazzler.xlsx')) as writer:
        n = 0
        for d in pd.date_range(viewers.index.min(), viewers.index.max()):
            day = d.date().isoformat()
            print(f'processing {day} for {sid}')
            viewsonday = viewers.loc[day]
            l = []
            for index, row in viewsonday.iterrows():
                l.append(watched(row, sched))
                n = n + 1
            sheet = pd.concat(l)
            sheet.index = sheet.index.tz_convert('Europe/London').tz_convert(None)
            sheet = sheet[['User ID', 'pid', 'Item Start', 'AV - Playback time', 'title', 'Session Start', 'Session End']]
            sheet.to_excel(writer, sheet_name=day, index=False)
        print(f'there are {len(viewers)} rows of data and {n} groups of processed data')

audience_data = sys.argv[1]
if len(sys.argv) > 2:
    sid = sys.argv[2]
    bucket = sys.argv[3]
viewers_for_all_channels = getaudiencedata(audience_data)
for vpid in config.keys():
    sid = config[vpid]['sid']
    viewers = viewers_for_all_channels[viewers_for_all_channels.version_id==vpid]
    report_for_channel(viewers, bucket, sid)

