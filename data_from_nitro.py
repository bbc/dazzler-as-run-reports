import os
import requests

apiKey = os.environ['API_KEY']

def resolve(vpid):
    r = requests.get(f'http://programmes.api.bbc.com/nitro/api/programmes?api_key={apiKey}&version={vpid}', headers={'Accept': 'application/json'})
    a = r.json()
    return a['nitro']['results']['items'][0]
