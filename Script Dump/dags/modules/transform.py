import pandas as pd
import re
from datetime import datetime

def format_numeric(x):
    """
    Formats a numeric string by removing all non-digit characters.

    Args:
        x (str): The input string to be formatted.

    Returns:
        str or None: The formatted string with all non-digit characters removed. If the input string is empty, None is returned.
    """
    x = re.sub(r"\D", "", x)
    
    return x if x else None
        
def format_date(x):
    """
    Formats a date string in the format "dd/mm/yyyy" to "dd-Mon-yyyy" format.

    Args:
        x (str): The date string to be formatted.

    Returns:
        str: The formatted date string in "dd-Mon-yyyy" format.
    """
    date = datetime.strptime(x, "%d/%m/%Y")
    return date.strftime("%d-%b-%Y")

def get_coe_duration(x):
    """
    Calculates the duration left in years for coe.

    Args:
        x (str): The target date in the format "%d-%b-%Y".

    Returns:
        float: The duration in years, rounded to one decimal place.
    """
    target_date = datetime.strptime(x.strip(), "%d-%b-%Y")
    current_date = datetime.now()
    duration = (target_date - current_date).days / 365
    return round(duration, 1)

def format_power(x):
    """
    Formats the power string by removing all non-digit characters and converting it to an integer.

    Args:
        x (str): The input string to be formatted.

    Returns:
        int or None: The formatted power value. If the input string is empty, None is returned.
    """
    x = re.sub(r"\D", "", x)
    
    return int(x) if x else None

def format_capacity(x):
    """
    Formats the capacity string by removing all non-digit characters and converting it to an integer.

    Args:
        x (str): The input string to be formatted.

    Returns:
        int or None: The formatted capacity value. If the input string is empty, None is returned.
    """
    x = re.sub(r"\D", "", x)
    
    return int(x) if x else None

def transform(df):
    """
    Transforms the given DataFrame by applying various formatting and filtering operations.

    Args:
        df (pandas.DataFrame): The input DataFrame to be transformed.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """
    df[['depreciation', 'price', 'milleage', 'omv', 'arf', 'power', 'capacity']] = df[['depreciation', 'price', 'milleage', 'omv', 'arf', 'power', 'capacity']].applymap(format_numeric)
    df[['registration_date', 'coe_expiry_date' ]] = df[['registration_date', 'coe_expiry_date']].applymap(format_date)
    df.loc[:, "coe_left"] = df.loc[:, "coe_expiry_date"].apply(get_coe_duration)
    df = df.loc[:, ['model', 'price', 'depreciation', 'milleage', 'registration_date', 'coe_left', 'no_of_owner', 'omv', 'arf', 'power', 'capacity', 'accessories', 'date']]
    df = df.dropna()
    
    dtype_conversion = {
    "model": str,
    "price": float,
    "depreciation": float,
    "milleage": float,
    "registration_date": str,
    "coe_left": float,
    "no_of_owner": int,
    "omv": float,
    "arf": float,
    "power": float,
    "capacity": float,
    "accessories": str,
    "date": str
    }

    # Convert data types
    df = df.astype(dtype_conversion)
    
    return df




