import pandas as pd
import src.extract as ext

df = ext.extract_data()

print(df.info())