import os
import urllib
import requests


opencagedata_key = os.getenv('opencagedata_key')
# print(opencagedata_key)


def find_coordinates(address, attribute):
    url = 'https://api.opencagedata.com/geocode/v1/json'
    query = {
        'q': f'{address}',
        'address_only': '1',
        'key': opencagedata_key,
    }
    url_enc = urllib.parse.urlencode(query)
    response = requests.get(url, params=url_enc)
    try:
        return response.json().get('results')[0]['geometry'][attribute]
    except IndexError:
        if response.status_code == 402:
            return 0.000000
        else:
            pass
