from typing import Dict
import os
from dotenv import dotenv_values
from datetime import datetime
import pandas as pd
import requests
import logging

config = {**dotenv_values('.env.shared')}

def setup_logging(log_file='app.log', level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(level)

    # Log to log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def get_aws_creds() -> Dict[str, str]:
    return {
        'aws_access_key_id': config['AWS_ACCESS_KEY_ID'],
        'aws_secret_access_key': config['AWS_SECRET_ACCESS_KEY']
    }

def get_bucket_name() -> str:
    return config['BUCKET_NAME']

def get_layer(layer: str) -> str:
    dir_path = f'data/{layer}'
    os.makedirs(dir_path, exist_ok=True)
    return dir_path

def read_csv(input_path: str, compression: str = 'gzip') -> pd.DataFrame:
    return pd.read_csv(input_path, compression=compression)

def write_to_sink(df: pd.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna()

def clean_account_id(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].astype('Int64')
    return df

def get_exchange_currency_rates() -> pd.DataFrame:
    api_url = "https://api.exchangerate-api.com/v4/latest/EUR"
    response = requests.get(api_url)
    if response.status_code == 200:
        rates = response.json()['rates']
        df = pd.DataFrame(rates.items(), columns=['currency', 'rate'])
        df['base_currency'] = 'EUR'
        df['timestamp_currency'] = datetime.now()
        return df
    else:
        print(f"Failed to fetch currency exchange data. Status code: {response.status_code}")
        return pd.DataFrame()

def calculate_amount_eur(df_transaction: pd.DataFrame, df_currency: pd.DataFrame) -> pd.DataFrame:
    # Convert to datetime types
    df_currency['timestamp_currency'] = pd.to_datetime(df_currency['timestamp_currency'])
    df_transaction['timestamp'] = pd.to_datetime(df_transaction['timestamp'])

    # Merge dataframes and calculate amount_eur
    merged_df = pd.merge(df_transaction, df_currency, on='currency')
    merged_df['amount_eur'] = merged_df['amount'] * merged_df['rate']

    return deduplicate(merged_df)

def join_login_transaction(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df1, df2, on='session_id', how='left')
    df.rename(columns={
        'account_id_x': 'account_id_login',
        'account_id_y': 'account_id_transaction',
        'timestamp_x': 'timestamp_login',
        'timestamp_y': 'timestamp_transaction'
    }, inplace=True)
    return df

def calculate_pdau_by_country(df: pd.DataFrame) -> pd.DataFrame:
    df['login_date'] = pd.to_datetime(df['timestamp_login']).dt.date
    df['transaction_date'] = pd.to_datetime(df['timestamp_transaction']).dt.date

    # Filter rows where login_date matches transaction_date
    filtered_df = df[df['login_date'] == df['transaction_date']]

    # Group and count
    pdaus_by_country = filtered_df.groupby(['country', 'login_date'])['account_id_login'].count().reset_index(name='pdau')
    total_logins = df.groupby(['country', 'login_date'])['account_id_login'].count().reset_index(name='total_logins')
    amount_eur = df.groupby(['country', 'login_date'])['amount_eur'].sum().reset_index(name='amount_eur')

    # Merge dataframes
    merged_df = pd.merge(pdaus_by_country, total_logins, on=['country', 'login_date'])
    merged_df = pd.merge(merged_df, amount_eur, on=['country', 'login_date'])

    # Calculate pdau_percentage
    merged_df['pdau_percentage'] = (merged_df['pdau'] / merged_df['total_logins']) * 100

    # Make a new column for week
    merged_df['login_date'] = pd.to_datetime(merged_df['login_date'])
    merged_df['login_week'] = merged_df['login_date'].apply(lambda x: x.isocalendar()[1])

    return merged_df