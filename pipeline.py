from datetime import datetime
import os
import boto3
import pandas as pd
from utils import *

events = ["transaction", "login"]
logger = setup_logging()
raw_layer = get_layer("raw")
bronze_layer = get_layer("bronze")
silver_layer = get_layer("silver")
gold_layer = get_layer("gold")


def get_s3_client():
    return boto3.client("s3", **get_aws_creds())

def download_s3_objects_to_raw(s3, bucket_name: str) -> None:
    objs = s3.list_objects_v2(Bucket=bucket_name)["Contents"]
    for obj in objs:
        output_path = f"{raw_layer}/{obj['Key'].replace('/', '_')}"
        s3.download_file(bucket_name, obj['Key'], output_path)
        logger.info(f"Downloaded {obj['Key']} to {output_path}")

def read_files_in_raw(event=None) -> pd.DataFrame:
    temp_dfs = []
    for filename in os.listdir(raw_layer):
        if event is None or event in filename:
            df = read_csv(f"{raw_layer}/{filename}")
            temp_dfs.append(df)
    return pd.concat(temp_dfs, ignore_index=True) if temp_dfs else pd.DataFrame()

def process_raw_and_save(df, output_path) -> None:
    write_to_sink(df, output_path)

def process_bronze_and_save(df: pd.DataFrame, filename: str, output_path: str, account_id_column: str = 'account_id') -> None:
    df = deduplicate(df)
    df = drop_nulls(df)
    df = clean_account_id(df, account_id_column)
    if 'transaction' in filename:
        df_currency = get_exchange_currency_rates()
        df = calculate_amount_eur(df, df_currency)
    write_to_sink(df, output_path)

def process_silver_and_save(df_login: pd.DataFrame, df_transaction: pd.DataFrame, output_path: str) -> pd.DataFrame:
    df = join_login_transaction(df_login, df_transaction)
    df = clean_account_id(df, 'account_id_transaction')
    write_to_sink(df, output_path)
    return df

def calculate_pdau_and_save(df: pd.DataFrame) -> pd.DataFrame:
    df_pdau = calculate_pdau_by_country(df)
    write_to_sink(df_pdau, f"{gold_layer}/pdau_by_country_login_date.csv.gz")
    return df_pdau

def load_to_raw() -> None:
    s3 = get_s3_client()
    download_s3_objects_to_raw(s3, get_bucket_name())

def load_to_bronze() -> None:
    for event in events:
        df = read_files_in_raw(event)
        output_path = f"{bronze_layer}/{event}.csv.gz"
        process_raw_and_save(df, output_path)
        logger.info(f"Processed all {event} files to {output_path}")

def load_to_silver() -> None:
    for filename in os.listdir(bronze_layer):
        input_path = f"{bronze_layer}/{filename}"
        df = read_csv(input_path)
        output_path = f"{silver_layer}/{filename}"
        process_bronze_and_save(df, filename, output_path)
        logger.info(f"Processed {input_path} to {output_path}")

def load_to_gold() -> pd.DataFrame:
    df_transaction = read_csv(f"{silver_layer}/transaction.csv.gz")
    df_login = read_csv(f"{silver_layer}/login.csv.gz")

    # Calculate pdau
    df_joined = process_silver_and_save(df_login, df_transaction, f"{gold_layer}/login_transaction.csv.gz")
    df_pdau = calculate_pdau_and_save(df_joined)
    logger.info(f"Finshed calculating PDAU")
    return df_pdau