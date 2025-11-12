import pandas as pd

# Caminho de um dos arquivos parquet da camada Silver
df = pd.read_parquet("dataset/silver/ano=2017/mes=01/dados.parquet")

# Mostrar as 5 primeiras linhas
print(df.head())

# Mostrar informações gerais
print(df.info())
