# silver_transformer.py
import json
from pathlib import Path
import pandas as pd
import numpy as np

BRONZE_DIR = Path("dataset/bronze")
SILVER_DIR = Path("dataset/silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def _read_all_parquets(base_dir):
    files = list(base_dir.rglob("*.parquet"))
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"⚠️ Erro ao ler {f}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def _detect_date_col(df):
    # preferência por coluna 'data', senão primeira coluna do tipo datetime ou com 'date' no nome
    if "data" in df.columns:
        return "data"
    for c in df.columns:
        if "date" in c.lower():
            return c
    # tenta inferir convertendo a primeira coluna que tenha muitos valores parecendo datas
    for c in df.columns:
        sample = df[c].dropna().astype(str).head(20).tolist()
        if any("-" in s or "/" in s for s in sample):
            return c
    return None

def _coerce_numeric_cols(df):
    numeric_cols = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            numeric_cols.append(c)
        else:
            # tenta converter colunas com muitos dígitos/virgulas/pontos
            sample = df[c].dropna().astype(str).head(50)
            if sample.str.match(r"^[\d\.\, \-]+$").sum() > 0:
                coerced = pd.to_numeric(df[c].astype(str).str.replace(r"[^\d\-,\.]", "", regex=True).str.replace(",", ".", regex=False), errors="coerce")
                if coerced.notna().sum() > 0:
                    df[c] = coerced
                    numeric_cols.append(c)
    return df, numeric_cols

def transform_bronze_to_silver(drop_duplicates_subset=None, required_columns=None):
    """
    Lê todos os parquets da pasta bronze, faz limpeza simples e salva particionado por ano/mes em dataset/silver.
    - drop_duplicates_subset: lista de colunas para considerar na remoção de duplicatas (opcional)
    - required_columns: lista de colunas críticas que não devem ficar nulas (opcional)
    """
    df = _read_all_parquets(BRONZE_DIR)
    if df.empty:
        print("⚠️ Nenhum dado encontrado em dataset/bronze.")
        return

    # 1) Coluna de data
    date_col = _detect_date_col(df)
    if date_col is None:
        raise RuntimeError("Não foi possível detectar a coluna de data. Inspecione seus parquets.")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "data"})

    # 2) Tipos numéricos
    df, numeric_cols = _coerce_numeric_cols(df)

    # 3) Tratar valores nulos
    # -- para colunas numéricas: preencher com 0 e registrar quantos foram preenchidos
    numeric_fill_report = {}
    for c in numeric_cols:
        nnull = df[c].isna().sum()
        if nnull > 0:
            numeric_fill_report[c] = int(nnull)
            df[c] = df[c].fillna(0)

    # -- para colunas não numéricas: preencher com 'unknown' quando necessário
    categorical_cols = [c for c in df.columns if c not in numeric_cols + ["data"]]
    cat_fill_report = {}
    for c in categorical_cols:
        nnull = df[c].isna().sum()
        if nnull > 0:
            cat_fill_report[c] = int(nnull)
            df[c] = df[c].fillna("unknown")

    # 4) Criar colunas de partição (ano/mes/dia/semana)
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["dia"] = df["data"].dt.day
    df["semana"] = df["data"].dt.isocalendar().week

    # 5) Remover duplicatas (se indicado) — por padrão tenta por todas as colunas
    if drop_duplicates_subset:
        before = len(df)
        df = df.drop_duplicates(subset=drop_duplicates_subset)
        after = len(df)
    else:
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
    dup_removed = before - after

    # 6) Testes simples de qualidade
    tests = {}
    # required columns não devem ter nulos
    if required_columns:
        for col in required_columns:
            tests[col] = int(df[col].isna().sum()) if col in df.columns else "MISSING_COLUMN"
    # verificar que ano/mes não têm nulos
    tests["ano_nulls"] = int(df["ano"].isna().sum())
    tests["mes_nulls"] = int(df["mes"].isna().sum())

    # 7) Salvar particionado por ano/mes na pasta silver
    for (ano, mes), grupo in df.groupby(["ano", "mes"]):
        out_dir = SILVER_DIR / f"ano={int(ano)}" / f"mes={int(mes):02}"
        out_dir.mkdir(parents=True, exist_ok=True)
        grupo.to_parquet(out_dir / "dados.parquet", index=False)

    # 8) Registrar um relatório simples
    report = {
        "rows_input": int(before),
        "rows_after_dedup": int(after),
        "duplicates_removed": int(dup_removed),
        "numeric_fill_report": numeric_fill_report,
        "categorical_fill_report": cat_fill_report,
        "tests": tests,
        "numeric_columns": numeric_cols,
        "categorical_columns": categorical_cols,
    }
    # salva relatório
    with open(SILVER_DIR / "silver_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("✅ Transformação para silver concluída. Relatório salvo em dataset/silver/silver_report.json")
    return report