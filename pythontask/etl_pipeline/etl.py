import pandas as pd
import logging
from .transformation_functions import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract(file_path: str) -> pd.DataFrame:

    try:
        deliveries = pd.read_json(file_path, orient='records')
        logging.info(f"Successfully extracted data from {file_path}. Rows: {len(deliveries)}")
        return deliveries
    except Exception as e:
        logging.error(f"Error during extraction from {file_path}: {e}")
        return pd.DataFrame()

def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    if df.empty:
        logging.warning("No data to transform. Skipping transformation step.")
        return pd.DataFrame()

    logging.info("Starting data transformation...")
    
    df['start_time'] = get_event_timestamp(df['events'], 'DELIVERY_STARTED')
    df['delivered_time'] = get_event_timestamp(df['events'], 'PACKAGE_DELIVERED')
  
    if 'scheduled_time' in df.columns:
        df['scheduled_time'] = pd.to_datetime(df['scheduled_time'], utc=True)

    df['delivery_duration_minutes'] = df.apply(calc_duration_minutes, axis=1)

    df['delivery_status'] = df.apply(calculate_delivery_status, axis=1)
    

    df = df.drop(columns=['events'], errors='ignore')
    df = df.rename(columns={'trackingId': 'delivery_id'})
    df = format_utc_cols_with_t_z(df,['scheduled_time','start_time','delivered_time'])

    logging.info("Transformation complete.")
    return df.copy()

def load(df: pd.DataFrame, output_path: str):

    if df.empty:
        logging.warning("No data to load. Skipping loading step.")
        return True # Considered successful if nothing to load
    try:
        logging.info(f"Attempting to save to: '{output_path}'")
        df.to_csv(output_path, index=False, na_rep='')
        logging.info(f"Successfully loaded data to {output_path}. Rows: {len(df)}")
        return True
    except Exception as e:
        logging.error(f"Error saving data to '{output_path}': {e}")
        return False