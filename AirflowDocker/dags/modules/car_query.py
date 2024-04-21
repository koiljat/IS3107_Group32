import requests

def get_car_query_trims(make, model, year=None, trim=None, body=None):
    BASE_URL = 'https://www.carqueryapi.com/api/0.3'

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    
    params = {
        'cmd': 'getTrims',
        'make': make,
        'model' : model
    }
    if year:
        params['year'] = year
    if trim:
        params['trim'] = trim
    if body:
        params['body'] = body
    try:
        response = requests.get(BASE_URL, params=params, headers = header)
        response.raise_for_status()
        return response.json()['Trims']  # Adjust based on the actual key for trims
    except requests.RequestException as e:
        print(e)
        return None 