import pandas as pd 
import numpy as np
import time

pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)

def parquetParaCsv(nomeArquivo):
  caminho = './dataset/{}'.format(nomeArquivo)
  arquivo = pd.read_parquet(caminho, engine="pyarrow")
  nomeArquivo = nomeArquivo.split('.')
  arquivo.to_csv('./dataset/{}.csv'.format(nomeArquivo[0]))


def csvParaParquet(caminhoCsv, caminhoParquet):
    arquivo = pd.read_csv(caminhoCsv)
    arquivo.to_parquet(caminhoParquet)


def procurarAnomalias(df):
    numLinhasInvalidas = 0
    cont = 0
    for (idx, linha) in df.iterrows():
        if(printarLinhaInvalida(linha, cont)):
            numLinhasInvalidas += 1
        cont += 1
    print(f"Linhas: {cont}")
    print(f"Linhas inválidas: {numLinhasInvalidas}")

def printarStringInvalida(valor, coluna, erroLinha):
    if (valor == "" or valor == None) :
        erroLinha += f"{coluna} vazia ou nula | "

def printarBooleanInvalido(valor, coluna, erroLinha):
    if (valor != 1 and valor != 0):
        erroLinha += f"{coluna} não é boolean | "

def printarLinhaInvalida(linha, numLinha):
    erroLinha = ""
    printarStringInvalida(linha[0], "ID", erroLinha)
    printarStringInvalida(linha[1], "accountID", erroLinha)
    printarStringInvalida(linha[2], "deviceID", erroLinha)
    printarStringInvalida(linha[3], "installationID", erroLinha)
    
    printarBooleanInvalido(linha[5], "isFromOfficialStore", erroLinha)
    printarBooleanInvalido(linha[6], "isEmulator", erroLinha)
    printarBooleanInvalido(linha[7], "hasFakeLocationApp", erroLinha)
    printarBooleanInvalido(linha[8], "hasFakeLocationEnabled", erroLinha)
    printarBooleanInvalido(linha[9], "probableRoot", erroLinha)
    printarBooleanInvalido(linha[12], "neverPermittedLocationOnAccount", erroLinha)
    printarBooleanInvalido(linha[16], "ato", erroLinha)
    
    if (erroLinha != ""): 
        print(numLinha + ": " + erroLinha)   
        return True
    return False   

def limparBd (df):
    print("Tamanho inicial: " + str(len(df)))
    inicio = time.time()
    colunas = df.columns
    indexes = []
    
    for (idx, row) in df.iterrows():
        print(idx)
        if (linhaCorreta(row) == False):
            indexes.append(idx)
        
    df.drop(indexes)
    print("Tamanho final: " + str(len(df)))    
    print("Demorou: " + str(time.time() - inicio) + "ms")
    df.to_parquet("teste.parquet4")

def stringValida(valores):
    for valor in valores:
        if valor == "" or valor == None:
            return False
    return True

def booleanValido(valores):
    for valor in valores:
        if valor != 1 and valor != 0:
            return False
    return True

def linhaCorreta(linha):
    verificarStrings = [linha[0], linha[1], linha[2], linha[3]]
    if not stringValida(verificarStrings):
        return False
    
    verificarBoolean = [linha[5], linha[6], linha[7], linha[8], linha[9], linha[12], linha[16]]
    if not booleanValido(verificarBoolean):
        return False
    
    return True

df = pd.read_parquet('./dataset/logins.parquet4')
#print(df.isna().sum())
print(len(df))
procurarAnomalias(df)