import pandas as pd

def extract_data():
    
    df = pd.read_parquet('data/raw/trip/yellow_tripdata_2024-01.parquet')

    return df