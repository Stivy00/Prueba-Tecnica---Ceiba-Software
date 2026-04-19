import os
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

# ── Configuración ────────────────────────────────────────────────
PROJECT_ID        = 'prueba-tecnica-ceiba-software'
RAW_DATASET       = "raw_fintrust"
STAGING_DATASET   = "stg_fintrust"
ANALYTICS_DATASET = "analytics_fintrust"
OUTPUT_DIR        = "docs"

CLIENT = bigquery.Client(project=PROJECT_ID)

# ── Utilidades ───────────────────────────────────────────────────

def run_query(sql: str) -> list[dict]:
    return [dict(row) for row in CLIENT.query(sql).result()]

def get_tables(dataset: str) -> list[str]:
    return [t.table_id for t in CLIENT.list_tables(CLIENT.dataset(dataset))]

def get_pk(dataset: str, table: str) -> str:
    return CLIENT.get_table(f"{PROJECT_ID}.{dataset}.{table}").schema[0].name

def get_required_cols(dataset: str, table: str) -> list[str]:
    table_ref = CLIENT.get_table(f"{PROJECT_ID}.{dataset}.{table}")
    return [f.name for f in table_ref.schema if f.mode == "REQUIRED"]

def build_result(dataset, tabla, control, columna, status, detalle) -> dict:
    return {
        "timestamp": datetime.now().isoformat(),
        "dataset"  : dataset,
        "tabla"    : tabla,
        "control"  : control,
        "columna"  : columna,
        "status"   : status,    # "OK" | "FAIL"
        "detalle"  : detalle,
    }

# ── Controles ────────────────────────────────────────────────────

def check_nulos(dataset: str, table: str) -> list[dict]:
    results = []
    for col in get_required_cols(dataset, table):
        sql = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{dataset}.{table}` WHERE {col} IS NULL"
        cnt = run_query(sql)[0]["cnt"]
        results.append(build_result(
            dataset, table, "Nulos", col,
            status  = "OK"   if cnt == 0 else "FAIL",
            detalle = "Sin nulos" if cnt == 0 else f"{cnt} filas con nulo",
        ))
    return results

def check_duplicados(dataset: str, table: str) -> list[dict]:
    pk  = get_pk(dataset, table)
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT {pk}, COUNT(*) AS c
            FROM `{PROJECT_ID}.{dataset}.{table}`
            GROUP BY {pk} HAVING c > 1
        )
    """
    cnt = run_query(sql)[0]["cnt"]
    return [build_result(
        dataset, table, "Duplicados", pk,
        status  = "OK"   if cnt == 0 else "FAIL",
        detalle = "Sin duplicados" if cnt == 0 else f"{cnt} PKs duplicadas",
    )]

def check_conteo_capas(table: str) -> list[dict]:
    tabla_raw     = table
    tabla_staging = f"stg_{table}"

    tablas_raw     = get_tables(RAW_DATASET)
    tablas_staging = get_tables(STAGING_DATASET)

    a = None
    b = None

    if tabla_raw in tablas_raw:
        sql = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{RAW_DATASET}.{tabla_raw}`"
        a   = run_query(sql)[0]["cnt"]

    if tabla_staging in tablas_staging:
        sql = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{STAGING_DATASET}.{tabla_staging}`"
        b   = run_query(sql)[0]["cnt"]

    if a is None or b is None:
        status  = "SKIP"
        detalle = f"Tabla no encontrada: raw={tabla_raw}, staging={tabla_staging}"
    elif a == b:
        status  = "OK"
        detalle = f"raw={a} | staging={b}"
    else:
        status  = "FAIL"
        detalle = f"raw={a} | staging={b} | diff={a - b}"

    return [build_result(
        dataset = f"{RAW_DATASET}→{STAGING_DATASET}",
        tabla   = f"{tabla_raw} → {tabla_staging}",
        control = "Conteo capas",
        columna = "-",
        status  = status,
        detalle = detalle,
    )]
# ── Orquestador ──────────────────────────────────────────────────

def ejecutar_controles():
    resultados = []
    datasets   = [RAW_DATASET, STAGING_DATASET, ANALYTICS_DATASET]

    for dataset in datasets:
        for tabla in get_tables(dataset):
            print(f"  Revisando {dataset}.{tabla}...")
            resultados += check_nulos(dataset, tabla)
            resultados += check_duplicados(dataset, tabla)

    for tabla in get_tables(RAW_DATASET):
        resultados += check_conteo_capas(tabla)

    # Exportar
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"quality_report_{timestamp}.xlsx")

    df       = pd.DataFrame(resultados)
    df_fails = df[df["status"] == "FAIL"]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Todos", index=False)
        df_fails.to_excel(writer, sheet_name="FAILS", index=False)

    print(f"\n✓ Reporte generado: {output_path}")
    print(f"  Total : {len(df)} | FAILs : {len(df_fails)} | OKs : {len(df[df['status'] == 'OK'])}")
    return df

if __name__ == "__main__":
    ejecutar_controles()