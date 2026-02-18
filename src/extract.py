import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_TRIP_DIR = BASE_DIR / 'data' / 'raw' / 'trip'
CLEAN_DIR = BASE_DIR / 'data' / 'clean'

def extract_data():
    
    df = pd.read_parquet(RAW_TRIP_DIR / 'yellow_tripdata_2024-01.parquet')
    return df