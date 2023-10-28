import boto3
import json

def mapitem(item):
    return {
        'start': item['start'],
        'title': item['title'],
        'end': item['end'],
        'vpid': item.get('version', {})['pid'],
        'pid': item.get('version_of', {})['pid'],
        'duration': item.get('version', {})['duration'],
    }

def get_planned(bucket, sid, day):
    s3 = boto3.client('s3')
    result = s3.get_object(Bucket=bucket, Key=f'{sid}/schedule/{day}-schedule.json')
    text = result["Body"].read().decode()
    s = json.loads(text)
    items = s['items']
    return [mapitem(i) for i in items]
