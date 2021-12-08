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
    np = df.to_numpy()
    for linha in np:    
        if(printarLinhaInvalida(linha, cont)):
            numLinhasInvalidas += 1
        cont += 1
    print(f"Linhas: {cont}")
    print(f"Linhas inválidas: {numLinhasInvalidas}")

def printarStringInvalida(valor, coluna):
    if (valor == "" or valor == None) :
        return f"{coluna} vazia ou nula | "
    return ""

def printarBooleanInvalido(valor, coluna):
    if (valor != 1 and valor != 0):
        return f"{coluna} não é boolean | "
    return ""

def printarLinhaInvalida(linha, numLinha):
    erroLinha = ""
    erroLinha += printarStringInvalida(linha[0], "ID")
    erroLinha += printarStringInvalida(linha[1], "accountID")
    erroLinha += printarStringInvalida(linha[2], "deviceID")
    erroLinha += printarStringInvalida(linha[3], "installationID")
    
    erroLinha += printarBooleanInvalido(linha[5], "isFromOfficialStore")
    erroLinha += printarBooleanInvalido(linha[6], "isEmulator")
    erroLinha += printarBooleanInvalido(linha[7], "hasFakeLocationApp")
    erroLinha += printarBooleanInvalido(linha[8], "hasFakeLocationEnabled")
    erroLinha += printarBooleanInvalido(linha[9], "probableRoot")
    erroLinha += printarBooleanInvalido(linha[12], "neverPermittedLocationOnAccount")
    erroLinha += printarBooleanInvalido(linha[16], "ato")
    
    if (erroLinha != ""): 
        print(str(numLinha) + ": " + erroLinha + "\n")   
        return True
    return False   

def limparBd (df):
    print("Tamanho inicial: " + str(len(df)))
    inicio = time.time()
    colunas = df.columns
    cont = 0
    indices = []
    dados = df.to_numpy()
    
    for linha in dados:
        if (linhaCorreta(linha) == False):
            indices.append(cont)
        cont += 1
    dados = np.delete(dados, indices, axis=0)
    df = pd.DataFrame(dados, columns=colunas)
    print("Qtd linhas inválidas: " + str(len(indices)))
    print("Tamanho final: " + str(len(df)))    
    print("Demorou: " + str(time.time() - inicio) + " segundos")
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
limparBd(df)