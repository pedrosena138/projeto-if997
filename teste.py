import pandas as pd 
import numpy as np
import time

def convertParquetToCsv(filename):
  path = './dataset/{}'.format(filename)
  file = pd.read_parquet(path, engine="pyarrow")
  filename = filename.split('.')
  file.to_csv('./dataset/{}.csv'.format(filename[0]))


def convertCsvToParquet(filepath, parquetPath):
    file = pd.read_csv(filepath)
    file.to_parquet(parquetPath)
pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)

def printAnomalousLine(linha):
    error = ""
    if linha[0] == "" or linha[0] == None:
        error += f"ID empty {str(linha[0])} | "
    if linha[1] == "" or linha[1] == None:
        error += f"accountID empty {str(linha[1])} | "
    if linha[2] == "" or linha[2] == None:
        error += f"deviceID empty {str(linha[2])} | "
    if linha[3] == "" or linha[3] == None:
        error += f"installationID empty {str(linha[3])} | "
    if linha[5] != 1 and linha[5] != 0:
        error += f"isFromOfficialStore not bool {str(linha[5])} | "
    if linha[6] != 1 and linha[6] != 0:
        error += f"isEmulator not bool {str(linha[6])} | "
    if linha[7] != 1 and linha[7] != 0:
        error += f"hasFakeLocationApp not bool {str(linha[7])} | "
    if linha[8] != 1 and linha[8] != 0:
        error += f"hasFakeLocationEnabled not bool {str(linha[8])} | "
    if linha[9] != 1 and linha[9] != 0:
        error += f"probableRoot not bool {str(linha[9])} | "
    if linha[12] != 1 and linha[12] != 0:
        error += f"neverPermittedLocationOnAccount not bool {str(linha[12])} | "
    if linha[16] != 1 and linha[16] != 0:
        error += f"ato not bool {str(linha[16])} | "
    
    if(error != ""):    
        return True
    return False

def searchAnomalies(parquetPath):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(parquetPath, engine="pyarrow")
    print("*--convertendo database--*")
    trash = dataframe.to_numpy()
    print("*--iniciando busca--*")
    nRowsErrors = 0
    count = 0
    for row in trash:        
        if(printAnomalousLine(row, count)):
            nRowsErrors += 1
        count += 1
    print(f"Numero de linhas: {count}")
    print(f"Numero de linhas falhas: {nRowsErrors}")

def limparBd (df):
    print("Tamanho inicial: " + str(len(df)))
    inicio = time.time()
    colunas = df.columns
    indexes = []
    
    for (idx, row) in df.iterrows():
      if (linhaCorreta(row) == False):
            indexes.append(idx)
        
    df.drop(indexes)
    print("Tamanho final: " + str(len(df)))    
    print("Demorou: " + str(time.time() - inicio) + "ms")
    df.to_parquet("teste.parquet4")

def linhaCorreta(linha):
    if linha[0] == "" or linha[0] == None:
        return False
    elif linha[1] == "" or linha[1] == None:
        return False
    elif linha[2] == "" or linha[2] == None:
        return False
    elif linha[3] == "" or linha[3] == None:
        return False
    elif linha[5] != 1 and linha[5] != 0:
        return False
    elif linha[6] != 1 and linha[6] != 0:
        return False
    elif linha[7] != 1 and linha[7] != 0:
        return False
    elif linha[8] != 1 and linha[8] != 0:
        return False
    elif linha[9] != 1 and linha[9] != 0:
        return False
    elif linha[12] != 1 and linha[12] != 0:
        return False
    elif linha[16] != 1 and linha[16] != 0:
        return False
    
    return True

df = pd.read_parquet('./dataset/logins.parquet4')
limparBd(df)