from datetime import datetime, timedelta
import pandas as pd
import sys

if __name__ == '__main__':
    key = sys.argv[1]
    df = pd.read_csv(key)
    cp = df[['channel_pid','audience_id','event_start_datetime_ms','playback_time']]
    cp["start"] = pd.to_datetime(cp["event_start_datetime_ms"]).round('1s')
    cp.start = cp.start.tz_localize('Europe/London')
    cp['duration']=cp['playback_time']
    cp = cp[['start','channel_pid','audience_id','duration']]
    cp = cp[cp.duration>0]
    range=pd.date_range(cp.index.min(), cp.index.max(), freq='s')
    grouped=cp.groupby(cp.audience_id, group_keys=True)
    padded = grouped.apply(lambda x: x.sort_index().reindex(range, method='pad'))
    print(padded)
    # cp.set_index('start',inplace=True)
    # cp=cp.tz_localize('Europe/London')

