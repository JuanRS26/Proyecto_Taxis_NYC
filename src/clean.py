import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_TRIP_DIR = BASE_DIR / 'data' / 'raw' / 'trip'
CLEAN_DIR = BASE_DIR / 'data' / 'clean'

# --------------------------------  DATAFAMES INFORMATION  --------------------------------------------------

# La siguiente lista contiene los nombres de los DataFrames que existen.
list_names_df = ['yellow_tripdata_2024-01.parquet', 'yellow_tripdata_2024-02.parquet', 
                'yellow_tripdata_2024-03.parquet', 'yellow_tripdata_2024-04.parquet',
                'yellow_tripdata_2024-05.parquet', 'yellow_tripdata_2024-06.parquet', 
                'yellow_tripdata_2024-07.parquet', 'yellow_tripdata_2024-08.parquet', 
                'yellow_tripdata_2024-09.parquet', 'yellow_tripdata_2024-10.parquet', 
                'yellow_tripdata_2024-11.parquet', 'yellow_tripdata_2024-12.parquet', 
                'yellow_tripdata_2025-01.parquet', 'yellow_tripdata_2025-02.parquet', 
                'yellow_tripdata_2025-03.parquet', 'yellow_tripdata_2025-04.parquet', 
                'yellow_tripdata_2025-05.parquet', 'yellow_tripdata_2025-06.parquet', 
                'yellow_tripdata_2025-07.parquet', 'yellow_tripdata_2025-08.parquet', 
                'yellow_tripdata_2025-09.parquet', 'yellow_tripdata_2025-10.parquet', 
                'yellow_tripdata_2025-11.parquet']

# La siguiente lista contiene los nombres de las columnas que existen en los DataFrames.
list_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
                'payment_type', 'fare_amount', 'extra','mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee']

# ------------------------------------  CLEANING  --------------------------------------------------

# La siguente funcion remplaza los valores nulos por valores especificos dependiendo de cada columna.
def replace_values_nan():
    
    for name_df in list_names_df:  # este ciclo itera todos los DataFrames que hay.
        
        df = pd.read_parquet(RAW_TRIP_DIR / name_df)    # se lee el DataFrame

        values = {'passenger_count': 0, 
                  'store_and_fwd_flag': 'N', 
                  'passenger_count': 0,
                  'RatecodeID': 99, 
                  'store_and_fwd_flag': 0, 
                  'congestion_surcharge': 0, 
                  'Airport_fee': 0}

        df.fillna(value = values, inplace = True)


# La siguiente funcion transforma el tipo de dato de las columnas
def transform_types():

    for name_df in list_names_df:
        
        df = pd.read_parquet(RAW_TRIP_DIR / name_df)

        df['VendorID'] = df['VendorID'].astype('int32')