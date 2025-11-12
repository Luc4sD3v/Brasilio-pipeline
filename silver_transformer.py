import pandas as pd
from pathlib import Path

# === CONFIGURAÇÃO DE CAMINHOS ===
BASE_DIR = Path(__file__).parent
BRONZE_DIR = BASE_DIR / "dataset" / "bronze"
SILVER_DIR = BASE_DIR / "dataset" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

print("🚀 Script silver_transformer.py iniciado!")
print("📁 Lendo dados de:", BRONZE_DIR.resolve())

# === LEITURA DOS ARQUIVOS PARQUET ===
arquivos = list(BRONZE_DIR.rglob("*.parquet"))
print(f"🔍 {len(arquivos)} arquivos encontrados.")

if not arquivos:
    print("⚠️ Nenhum arquivo encontrado na pasta Bronze.")
else:
    # === CARREGAR TODOS OS ARQUIVOS ===
    dfs = []
    for arquivo in arquivos:
        df = pd.read_parquet(arquivo)
        dfs.append(df)

    dados = pd.concat(dfs, ignore_index=True)
    print(f"📊 Total de registros carregados: {len(dados)}")

    # === LIMPEZA E TRATAMENTO DE DADOS ===
    print("🧹 Limpando e padronizando os dados...")

    # Remover colunas completamente vazias
    dados = dados.dropna(how="all", axis=1)

    # Preencher valores nulos em colunas numéricas com 0
    for col in dados.select_dtypes(include=["float", "int"]).columns:
        dados[col] = dados[col].fillna(0)

    # Preencher valores nulos em colunas de texto com "Não informado"
    for col in dados.select_dtypes(include=["object"]).columns:
        dados[col] = dados[col].fillna("Não informado")

    # Converter datas
    if "data" in dados.columns:
        dados["data"] = pd.to_datetime(dados["data"], errors="coerce")
        dados["ano"] = dados["data"].dt.year
        dados["mes"] = dados["data"].dt.month

    # === VERIFICAÇÃO DE QUALIDADE SIMPLES ===
    print("🔍 Verificando qualidade dos dados...")
    if "ano" in dados.columns and dados["ano"].isnull().any():
        print("⚠️ Existem registros sem ano válido!")
    if "mes" in dados.columns and dados["mes"].isnull().any():
        print("⚠️ Existem registros sem mês válido!")

    # === SALVAR NA CAMADA SILVER ===
    print("💾 Salvando dados tratados na camada Silver...")
    for (ano, mes), grupo in dados.groupby(["ano", "mes"]):
        pasta_saida = SILVER_DIR / f"ano={ano}" / f"mes={mes:02}"
        pasta_saida.mkdir(parents=True, exist_ok=True)
        grupo.to_parquet(pasta_saida / "dados.parquet", index=False)

    print("✅ Dados tratados e salvos com sucesso na camada Silver!")
    print("🎉 Pipeline Silver concluída com sucesso!")