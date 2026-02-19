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

dates = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
         '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
         '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01', 
         '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01']

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


def dates_correction():

    column_pickup = 'tpep_pickup_datetime'
    columna_dropoff = 'tpep_dropoff_datetime'
    
    print(f'Se realizara la correccion de las fechas de los DataFrames para la columna "{column_pickup}"...\n')
    
    for i, name_df in enumerate(list_names_df):

        df = pd.read_parquet(RAW_TRIP_DIR / name_df)

        total_values = df[column_pickup].count()

        df = df[(df[column_pickup] >= dates[i]) & (df[column_pickup] < dates[i+1])]

        correct_values = df[column_pickup].count()
        deleted_values = total_values - correct_values

        print(f'{name_df} -> Total: {total_values} / Correct: {correct_values} / Deleted: {deleted_values}')


    print(f'\nSe realizara la correccion de las fechas de los DataFrames para la columna "{columna_dropoff}"...\n')
    
    for i, name_df in enumerate(list_names_df):

        df = pd.read_parquet(RAW_TRIP_DIR / name_df)

        total_values = df[columna_dropoff].count()

        df = df[df[columna_dropoff] >= dates[i]]

        correct_values = df[columna_dropoff].count()
        deleted_values = total_values - correct_values

        print(f'{name_df} -> Total: {total_values} / Correct: {correct_values} / Deleted: {deleted_values}')