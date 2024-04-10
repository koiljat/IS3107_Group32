
import requests

def get_jwt_token():
    BASE_URL = "https://carapi.app/api"
    login_url = f"{BASE_URL}/auth/login"
    credentials = {
        "api_token" : "780ed9d1-727c-410f-b73e-2bed8b05c11a",
        "api_secret" : "9bdf99f4de7a672d0768761df407ab60"
    }
    response = requests.post(login_url,json=credentials)
    response.raise_for_status()
    return response.text

def get_trim_ids_for_make_model(make, model, year):
    jwt_token = get_jwt_token()
    BASE_URL = "https://carapi.app/api"
    url = f"{BASE_URL}/trims"
    headers = {'Authorization': f'Bearer {jwt_token}'}
    params = {'make': make, 'model': model, 'year': year}
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()

    trim_ids = [trim['id'] for trim in data['data']]  # Adjusted to access the 'trims' key
    if trim_ids:
        trim_id = trim_ids[0]
    else:
        trim_id = None

    return trim_id

def get_trim_details(make, model, year):

    trim_id = get_trim_ids_for_make_model(make, model, year)
    if trim_id:
        jwt_token = get_jwt_token()
        BASE_URL = "https://carapi.app/api"
        url = f"{BASE_URL}/trims/{trim_id}"
        headers = {'Authorization': f'Bearer {jwt_token}'}
    
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    
        data = response.json()
        if data['make_model_trim_body']['curb_weight']:
            data['make_model_trim_body']['curb_weight'] = float(data['make_model_trim_body']['curb_weight']) / 2.205
        
        if data['make_model_trim_mileage']['fuel_tank_capacity']:
            data['make_model_trim_mileage']['fuel_tank_capacity'] = float(data['make_model_trim_mileage']['fuel_tank_capacity']) * 3.785
            

        return data
    else:
        return None
