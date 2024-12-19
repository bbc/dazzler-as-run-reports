import pandas as pd
from getathena import get_data

if __name__ == '__main__':
    ar1=pd.read_parquet('s3://iplayer-dazzler-asruns/daily/eu-west-1_2024-12-15.gz')
    ar2=pd.read_parquet('s3://iplayer-dazzler-asruns/daily/eu-west-2_2024-12-15.gz')
    ar = pd.concat([ar1, ar2])
    day='2024-12-15'
    e='2024-12-16'
    lt = ar.loc[ar['channel_vpid']=='p0gh5q9m']
    lt.index = lt['start']
    lt.index = lt.index.tz_localize('Europe/London')
    asrun = lt.reindex(pd.date_range(day, e, freq='T', tz='Europe/London'), method='pad')
    print(asrun.info())
    m = asrun.reset_index()
    ad = pd.DataFrame(get_data('20241215'))
    stats = m.merge(ad.reset_index(), left_index=True, right_index=True)
    stats.index = stats['index_x']
    stats.index = stats.index.tz_localize(None)
    r = stats.drop(columns=['index_x','index_y', 'duration', 'start', 'delta', 'direction'])
    print(r.info())