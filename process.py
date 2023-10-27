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

def addpids(audience_row, schedule):
    s = audience_row['start']
    e = audience_row['end']
    pids = schedule.loc[s:e].copy().tz_convert('Europe/London')
    pids['User ID'] = audience_row['User ID']
    return pids

audience_data = sys.argv[1]
sid = sys.argv[2]
bucket = sys.argv[3]
augmented = audience_data.replace('.csv', '-dazzler.xlsx')
viewers = pd.read_csv(audience_data, header=0, index_col=2, dtype={'a': str, 'b': str, 'c': object, 'd': np.float64, 'e': np.int32, 'f': np.float64}, parse_dates=True)
viewers['start'] = pd.to_datetime(viewers.index).tz_localize('Europe/London')
viewers['end'] = viewers['start'] + pd.to_timedelta(viewers['AV - Playback time'], unit='ms')
s = viewers['start'].min()
e = viewers['start'].max()
sd = s.date().isoformat()
ed = e.date().isoformat()
print(f'stats from {sd} to {ed}')
with pd.ExcelWriter(augmented) as writer:
    l = []
    for d in pd.date_range(sd, ed):
        day = d.date().isoformat()
        print(f'processing {day}')
        data = get_planned(bucket, sid, day)
        sched = pd.DataFrame.from_records(data, index='start')
        sched.index = pd.to_datetime(sched.index)
        sched['start'] = sched.index
        sched['end'] = pd.to_datetime(sched['end'])
        viewsonday = viewers.loc[day]
        for index, row in viewsonday.iterrows():
            views = addpids(row, sched).tz_convert('Europe/London').tz_localize(None)
            l.append(views)
    print(f'there are {len(viewers)} rows of data and {len(l)} groups of processed data')
    sheet = pd.concat(l)
    sheet = sheet[['User ID', 'epid', 'title']]
    sheet.to_excel(writer, sheet_name=f'{sd}-{ed}', index=True)
