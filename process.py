import pandas as pd
import numpy as np
import sys
from datetime import datetime, timedelta
from ml_asruns import get_as_run
from dazzler_schedule import get_planned

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
    wanted['User ID'] = audience_row['User ID']
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

def getaudiencedata(filename):
    viewers = pd.read_csv(filename, header=0, index_col=2, dtype={'a': str, 'b': str, 'c': object, 'd': np.float64, 'e': np.int32, 'f': np.float64}, parse_dates=True)
    viewers.index = viewers.index.tz_localize('Europe/London')
    viewers['start'] = viewers.index
    viewers['end'] = viewers.index + pd.to_timedelta(viewers['AV - Playback time'], unit='ms')
    return viewers

audience_data = sys.argv[1]
sid = sys.argv[2]
bucket = sys.argv[3]
viewers = getaudiencedata(audience_data)
sched = getscheduleforsessions(viewers, bucket, sid)
with pd.ExcelWriter(audience_data.replace('.csv', '-dazzler.xlsx')) as writer:
    n = 0
    for d in pd.date_range(viewers.index.min(), viewers.index.max()):
        day = d.date().isoformat()
        print(f'processing {day}')
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
