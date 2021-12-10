def parquetParaCsv(nomeArquivo):
    caminho = './dataset/{}{}'.format(nomeArquivo, ".parquet4")
    arquivo = pd.read_parquet(caminho, engine="pyarrow")
    nomeArquivo = nomeArquivo.split('.')
    arquivo.to_csv('./dataset/{}.csv'.format(nomeArquivo[0]))
    return arquivo


def csvParaParquet(caminhoCsv, caminhoParquet):
    arquivo = pd.read_csv(caminhoCsv)
    arquivo.to_parquet(caminhoParquet)
    return arquivo