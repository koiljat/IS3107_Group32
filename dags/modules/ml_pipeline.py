import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
import numpy as np

def data_encoding(data):
    for index, row in data.iterrows():
        row['model_body'] = str(row['model_body']).lower()
        if 'sport' in row['model_body']:
            data.at[index, 'model_body'] ='SUV'
        elif 'compact' in row['model_body'] or 'subcompact' in row['model_body']:
            data.at[index, 'model_body'] ='Compact Cars'
        elif 'manual' in row['model_body']:
            data.at[index, 'model_body'] ='Manual'
        elif 'wagon' in row['model_body']:
            data.at[index, 'model_body'] ='Wagon'

    data = pd.get_dummies(data, columns=['model_body'], drop_first=True)

    for index, row in data.iterrows():
        row['model_transmission_type'] = str(row['model_transmission_type']).lower()
        if 'continuously' in row['model_transmission_type'] or 'single Speed' in row['model_transmission_type'] or 'cvt' in row['model_transmission_type']:
            data.at[index, 'model_transmission_type'] ='CVT'
        elif 'automatic' in row['model_transmission_type']:
            data.at[index, 'model_transmission_type'] ='Automatic'
        elif 'manual' in row['model_transmission_type']:
            data.at[index, 'model_transmission_type'] ='Manual'
        elif 'automated manual' in row['model_transmission_type']:
            data.at[index, 'model_transmission_type'] ='Automated Manual'
        else:
            data.at[index, 'model_transmission_type'] ='Others'

    data = pd.get_dummies(data, columns=['model_transmission_type'], drop_first=True)

    for index, row in data.iterrows():
        row['model_drive'] = str(row['model_drive']).lower()
        if 'all' in row['model_drive'] or '4wd' in row['model_drive'] or 'awd' in row['model_drive'] or 'four' in row['model_drive']:
            data.at[index, 'model_drive'] ='All Wheel Drive'
        elif 'front' in row['model_drive']:
            data.at[index, 'model_drive'] ='Front Wheel Drive'
        elif 'rear' in row['model_drive']:
            data.at[index, 'model_drive'] ='Rear Wheel Drive'
        else:
            data.at[index, 'model_drive'] ='Others'

    data = pd.get_dummies(data, columns=['model_drive'], drop_first=True)

    for index, row in data.iterrows():
        row['model_engine_fuel'] = str(row['model_engine_fuel']).lower()
        if 'premium' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Premium'
        elif 'regular' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Regular'
        elif 'hybrid' in row['model_engine_fuel'] or 'gasoline' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Hybrid'
        elif 'gasoline' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Regular'
        elif 'diesel' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Diesel'
        elif 'electric' in row['model_engine_fuel']:
            data.at[index, 'model_engine_fuel'] ='Electric'
        else:
            print(row['model_engine_fuel'])
            data.at[index, 'model_engine_fuel'] ='Others'

    data = pd.get_dummies(data, columns=['model_engine_fuel'], drop_first=True)

    data['model_engine_cyl'] = data['model_engine_cyl'].astype("string")
    for index, row in data.iterrows():
        row['model_engine_cyl'] = str(row['model_engine_cyl']).lower()
        if '2' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='2'
        elif '3' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='3'
        elif '4' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='4'
        elif '5' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='5'
        elif '6' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='6'
        elif '8' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='8'
        elif '10' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='10'
        elif '12' in row['model_engine_cyl']:
            data.at[index, 'model_engine_cyl'] ='12'

    data['model_engine_cyl'] = data['model_engine_cyl'].astype("Int64")

    brands = {
        'budget' : ['Chevrolet', 'Citroen', 'Fiat', 'Ford', 'Honda', 'Hyundai', 'Kia', 'Mazda', 'Mitsubishi', 'Nissan', 'Peugeot', 'Renault', 'Skoda', 'Ssangyong', 'Subaru', 'Suzuki', 'Toyota', 'Daihatsu', 'Proton'],
        'mid' : ['Alfa Romeo', 'Chrysler', 'Infiniti', 'MINI', 'Opel', 'Saab', 'Volkswagen', 'Audi', 'BMW', 'Jaguar', 'Jeep', 'Lexus', 'Lotus', 'Mercedes-Benz', 'Mercedes Benz', 'Mitsuoka', 'Volvo', 'Dodge Journey'],
        'exotic' : ['Aston Martin', 'Ferrari', 'Lamborghini', 'McLaren', 'Bentley', 'Land-Rover', 'Land Rover', 'Maserati', 'Porsche', 'Rolls-Royce', 'Rolls Royce']
        }
    
    data['brands'] = "others"

    data['brands'] = "others"

    for index, row in data.iterrows():
        model = str(row['make'])
        for group, brand_list in brands.items():
            for brand in brand_list:
                if brand.lower() in model.lower():
                    data.at[index, 'brands'] = group

    ordinal_mapping = {
        'budget': 1,
        'mid': 2,
        'exotic' : 3,
        'others' : 1.5
        }
    data['brands'] = data['brands'].map(ordinal_mapping)
    data = data.drop(columns=['make'])
    return data

def drop_cols(data):
    columns_to_drop = ['model_trim', 'accessories', 'model_make_id', 'vehicle_class', 'month', 'date_listed', 'bidding_no']
    data = data.drop(columns=columns_to_drop)
    return data

def change_reg_date_to_years(data):
    data['years_since_reg'] = 0

    for index, row in data.iterrows():
        year = row['model_year']
        data.at[index, 'years_since_reg'] = 2024-year

    data = data.drop(columns=['model_year'])
    return data

def drop_highly_correlated_cols(data):

    independent_vars = data.drop(columns=['price'])

    correlation = independent_vars.corr()

    threshold = 0.7
    upper = correlation.where(np.triu(np.ones(correlation.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if any(upper[column].abs() > threshold)]
    data = data.drop(columns=to_drop)

    return data

def train_evaluate_GB(x,y):
    X_GB_train, X_GB_test, y_GB_train, y_GB_test = train_test_split(x, y, test_size=0.2, random_state=42)
    gb_regressor = GradientBoostingRegressor(n_estimators=100, random_state=42)  # 100 trees
    gb_regressor.fit(X_GB_train, y_GB_train)
    y_GB_pred = gb_regressor.predict(X_GB_test)
    r2_GB = r2_score(y_GB_test, y_GB_pred)

    return r2_GB, gb_regressor



def train_evaluate_DT(x,y):
    X_DT_train, X_DT_test, y_DT_train, y_DT_test = train_test_split(x, y, test_size=0.2, random_state=42)
    dt_regressor = DecisionTreeRegressor(random_state=42)
    dt_regressor.fit(X_DT_train, y_DT_train)
    y_DT_pred = dt_regressor.predict(X_DT_test)

    r2_DT = r2_score(y_DT_test, y_DT_pred)

    return r2_DT, dt_regressor

def train_evaluate_RF(x,y):
    X_RF_train, X_RF_test, y_RF_train, y_RF_test = train_test_split(x, y, test_size=0.2, random_state=42)
    rf_regressor = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_regressor.fit(X_RF_train, y_RF_train)
    y_rf_pred = rf_regressor.predict(X_RF_test)

    r2_RF = r2_score(y_RF_test, y_rf_pred)

    return r2_RF, rf_regressor
