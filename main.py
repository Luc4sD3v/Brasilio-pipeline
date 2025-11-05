import requests
import json
import time
import pandas as pd
from pathlib import Path

# === CONFIGURAÇÃO ===
BASE_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"
TOKEN = "TOKEN AQUI" 
HEADERS = {"Authorization": f"Token {TOKEN}"}

# === ESTRUTURA DE PASTAS ===
RAW_DIR = Path("dataset/raw")
BRONZE_DIR = Path("dataset/bronze")
RAW_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# === BAIXAR DADOS (até a página 1000) ===
def baixar_dados(limite_paginas=1000):
    url = BASE_URL
    page = 1
    while url and page <= limite_paginas:
        arquivo = RAW_DIR / f"pagina_{page:03}.json"
        if arquivo.exists():
            print(f"⏩ Página {page} já existe, pulando...")
            page += 1
            url = f"{BASE_URL}/?page={page}"
            continue

        print(f"📥 Baixando página {page}...")
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 429:
            print("⚠️  Muitas requisições — esperando 10s...")
            time.sleep(10)
            continue

        r.raise_for_status()
        data = r.json()

        # salva os dados brutos
        with open(arquivo, "w", encoding="utf-8") as f:
            json.dump(data["results"], f, ensure_ascii=False, indent=2)

        url = data["next"]
        page += 1
        time.sleep(2)

    print(f"✅ Download concluído! ({page-1} páginas processadas)")

# === CONVERTER PARA PARQUET ===
def converter_para_parquet():
    print("🧩 Convertendo JSONs para Parquet...")
    dfs = []

    for arquivo in RAW_DIR.glob("*.json"):
        with open(arquivo, "r", encoding="utf-8") as f:
            dados = json.load(f)
        df = pd.DataFrame(dados)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
            df["ano"] = df["data"].dt.year
            df["mes"] = df["data"].dt.month
        dfs.append(df)

    final = pd.concat(dfs, ignore_index=True)

    for (ano, mes), grupo in final.groupby(["ano", "mes"]):
        pasta = BRONZE_DIR / f"ano={ano}" / f"mes={mes:02}"
        pasta.mkdir(parents=True, exist_ok=True)
        grupo.to_parquet(pasta / "dados.parquet", index=False)

    print("✅ Dados salvos em 'dataset/bronze'")

# === EXECUÇÃO AUTOMÁTICA ===
if __name__ == "__main__":
    print("🚀 Iniciando pipeline automatizada (até página 1000)...")
    try:
        baixar_dados(limite_paginas=1000)   
        converter_para_parquet()            
        print("🎉 Pipeline finalizada com sucesso!")
    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
