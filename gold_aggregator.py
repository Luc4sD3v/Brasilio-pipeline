import pandas as pd
from pathlib import Path

# === CONFIGURAÇÃO DOS CAMINHOS ===
BASE_DIR = Path(__file__).parent
SILVER_DIR = BASE_DIR / "dataset" / "silver"
GOLD_DIR = BASE_DIR / "dataset" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

print("🚀 Script gold_aggregator.py iniciado!")
print("📁 Lendo dados da camada Silver:", SILVER_DIR.resolve())

# === LER TODOS OS ARQUIVOS PARQUET DA SILVER ===
arquivos = list(SILVER_DIR.rglob("*.parquet"))
print(f"🔍 {len(arquivos)} arquivos encontrados.")

if not arquivos:
    print("⚠️ Nenhum arquivo encontrado na pasta Silver.")
    exit()

dfs = []
for arquivo in arquivos:
    df = pd.read_parquet(arquivo)
    dfs.append(df)

dados = pd.concat(dfs, ignore_index=True)
print(f"📊 Total de registros carregados: {len(dados):,}")

# === IDENTIFICAR NOME CORRETO DA COLUNA DE VALOR ===
possiveis_colunas_valor = ["valor_pago", "valor_liquido", "valor", "vlr_pago"]
coluna_valor = next((col for col in possiveis_colunas_valor if col in dados.columns), None)

if not coluna_valor:
    print("❌ Nenhuma coluna de valor encontrada! Verifique o nome da coluna no dataset Silver.")
    print("🔍 Colunas disponíveis:", list(dados.columns))
    exit()

print(f"💰 Usando a coluna de valor: '{coluna_valor}'")

# === ANÁLISE EXPLORATÓRIA OTIMIZADA ===
print("\n📈 Análise exploratória inicial:")

if len(dados) < 100000:
    # Dataset pequeno → mostra estatísticas completas
    print(dados.describe(include="all").transpose().head(10))
else:
    # Dataset grande → mostra apenas amostra e tipos
    print("📊 Dataset grande — mostrando apenas uma amostra e os tipos de dados:")
    print(dados.dtypes.head(10))
    print("\n🔍 Amostra de 5 linhas:")
    print(dados.head())

# === AGREGAÇÕES (ANÁLISE DE NEGÓCIO) ===

# 1️⃣ Gasto total por ano e mês
if "ano" in dados.columns and "mes" in dados.columns:
    gastos_mensais = (
        dados.groupby(["ano", "mes"], dropna=False)
        .agg(total_gasto=(coluna_valor, "sum"))
        .reset_index()
        .sort_values(["ano", "mes"])
    )
else:
    print("⚠️ Colunas 'ano' e 'mes' não encontradas para agregação mensal.")
    gastos_mensais = pd.DataFrame()

# 2️⃣ Gasto total por órgão superior (se existir)
if "orgao_superior" in dados.columns:
    gastos_orgao = (
        dados.groupby("orgao_superior", dropna=False)
        .agg(total_gasto=(coluna_valor, "sum"))
        .reset_index()
        .sort_values("total_gasto", ascending=False)
    )
else:
    gastos_orgao = pd.DataFrame()

# === SALVAR RESULTADOS NA CAMADA GOLD ===
print("\n💾 Salvando agregações na camada Gold...")

# Salvando por partição (ano/mês)
if not gastos_mensais.empty:
    for (ano, mes), grupo in gastos_mensais.groupby(["ano", "mes"]):
        pasta_saida = GOLD_DIR / f"ano={ano}" / f"mes={mes:02}"
        pasta_saida.mkdir(parents=True, exist_ok=True)
        grupo.to_parquet(pasta_saida / "gastos_mensais.parquet", index=False)
    print("✅ Gastos mensais salvos com sucesso.")

# Salvando agregação por órgão
if not gastos_orgao.empty:
    gastos_orgao.to_parquet(GOLD_DIR / "gastos_por_orgao.parquet", index=False)
    print("✅ Gastos por órgão superior salvos com sucesso.")

print("\n🎉 Pipeline Gold concluída com sucesso!")
