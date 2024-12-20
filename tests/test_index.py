import json
from moto import mock_aws
from unittest.mock import patch
from datetime import datetime
import pandas as pd
from index import unixtime, gen, cmap, get_as_run, get_channels, get_simple_as_run_report, make_one_second_data_for_channel, make_one_second_data, main

start = datetime.fromisoformat('2024-12-15T00:00:00Z')
end = datetime.fromisoformat('2024-12-15T00:02:00Z')
region = 'eu-west-1'

idf = pd.DataFrame([
    {'name': 'hist', 'start': '2024-12-15T00:00:00Z', 'duration': 'PT1M', 'channel_vpid': 'cv1', 'item_vpid': 'vp1'},
    {'name': 'hist', 'start': '2024-12-15T00:01:00Z', 'duration': 'PT1M', 'channel_vpid': 'cv1', 'item_vpid': 'vp2'},
    {'name': 'xmas', 'start': '2024-12-15T00:00:00Z', 'duration': 'PT1M', 'channel_vpid': 'cv2', 'item_vpid': 'vp3'},
])
df=idf.astype(
    {'name': 'string', 'start': 'datetime64[ns, UTC]', 'duration': 'timedelta64[ns]', 'channel_vpid': 'string', 'item_vpid': 'string'}
)

def test_unixtime():
    assert unixtime(datetime.fromisoformat('2023-01-01T00:00:00Z')) == 1672531200000

def test_gen():
    assert [i for i in gen([])] == []
    assert [i for i in gen([{'events':[{'message': '""'}]}])] == [""]
    assert [i for i in gen([{'events':[{'message': '""'}]}, {'events':[{'message': '{}'}]}])] == ["",{}]


def test_cmap(): 
    assert cmap({'Name': 'X Y', 'Arn': 'somearn', 'Tags': {}}) == {'name': 'Y', 'arn': 'somearn'}
    assert cmap({'Name': 'X Y', 'Arn': 'somearn', 'Tags': {'vpid': 'p'}}) == {'name': 'Y', 'arn': 'somearn', 'vpid': 'p'}

@mock_aws
def test_get_as_run(): 
    assert get_as_run(region, 'aaa', start, end) is not None

@mock_aws
def test_get_channels(): 
    assert get_channels(region) is not None

@patch('index.get_as_run')
def test_get_simple_as_run_report_empty(mock_get_as_run): 
    mock_get_as_run.return_value=[]
    assert [c for c in get_simple_as_run_report(region, [], start, end)] == []
    assert [c for c in get_simple_as_run_report(region, [{'arn':':::::::', 'vpid': '', 'name': ''}], start, end)] == []

@patch('index.get_as_run')
def test_get_simple_as_run_report_not_empty(mock_get_as_run): 
    message = {'action_name': '20231201000000 PT1M sched avpid'}
    event = {'channel_arn':'','timestamp':'T','message': json.dumps(message)}
    mock_get_as_run.return_value=[{'events': [{'message': json.dumps(event)}]}]
    assert [c for c in get_simple_as_run_report(region, [{'arn':':::::::', 'vpid': '', 'name': ''}], start, end)] == [{'name': '', 'start': 'TZ', 'duration': 'PT1M', 'channel_vpid': '', 'item_vpid': 'avpid'}]

def test_make_one_second_data_for_channel(snapshot): 
    input = df[df['channel_vpid']=='cv1']
    e = pd.to_datetime(end).tz_convert('Europe/London')
    s = pd.to_datetime(start).tz_convert('Europe/London')
    r = make_one_second_data_for_channel(input, s, e)
    snapshot.assert_match(r.to_csv(index_label='start'), 'make_one_second_data_for_channel.csv')

def test_make_one_second_data(snapshot):
    s = pd.to_datetime(start).tz_convert('Europe/London')
    e = pd.to_datetime(end).tz_convert('Europe/London')
    r = make_one_second_data([{'vpid': 'cv1'},{'vpid': 'cv2'}], s, e, df)
    snapshot.assert_match(r.to_csv(index_label='start'), 'make_one_second_data.csv')

def test_main():
    pass