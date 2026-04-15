import os
from dotenv import load_dotenv
import requests

load_dotenv()
key = os.getenv('FRED_API_KEY')
series = ['PWHEAMTUSDM', 'PMAIZMTUSDM', 'PSOYBUSDQ', 'PPIACO', 'DHHNGSP', 'DHOILNYH']

res = {}
for s in series:
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={key}&file_type=json'
    try:
        data = requests.get(url).json().get('observations', [])
        if data:
            # check the frequency and start/end dates
            info_url = f'https://api.stlouisfed.org/fred/series?series_id={s}&api_key={key}&file_type=json'
            info = requests.get(info_url).json().get('seriess', [{}])[0]
            frequency = info.get('frequency', 'Unknown')
            res[s] = {'freq': frequency, 'start': data[0]['date'], 'end': data[-1]['date'], 'count': len(data)}
        else:
            res[s] = 'No data'
    except Exception as e:
        res[s] = str(e)

import pprint
pprint.pprint(res)
