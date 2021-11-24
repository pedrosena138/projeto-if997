import pandas as pd 
import numpy as np

def convertParquetToCsv(filename: str):
  path = './dataset/{}'.format(filename)
  file = pd.read_parquet(path, engine="pyarrow")
  filename = filename.split('.')
  file.to_csv('./dataset/{}.csv'.format(filename[0]))


def convertCsvToParquet(filepath: str, parquetPath: str):
    file = pd.read_csv(filepath)
    file.to_parquet(parquetPath)
pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)

df = pd.read_parquet('./dataset/logins.parquet4')
print(df.head(5))