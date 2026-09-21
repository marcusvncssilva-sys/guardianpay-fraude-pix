"""
Script auxiliar só para conferir visualmente que as métricas de cliente
e a correlação bivariada foram gravadas corretamente. Não faz parte do
pipeline — é só uma ferramenta de inspeção manual.

Uso (dentro do container):
    python3 /opt/spark_jobs/verificar_novas_metricas.py 2026-09-19
"""
import sys
import json

from pyspark.sql import SparkSession

data_ref = sys.argv[1] if len(sys.argv) > 1 else "2026-09-19"
lake_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/data/lake"

spark = SparkSession.builder.master("local[1]").appName("verificacao").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 70)
print(f"Verificando data_ref={data_ref} em {lake_path}")
print("=" * 70)

df = spark.read.parquet(f"{lake_path}/silver/transacoes").filter(f"data_ref = '{data_ref}'")

print("\n--- Colunas presentes na Silver ---")
for c in ["qtd_transacoes_cliente", "valor_medio_cliente"]:
    print(f"  {c}: {'PRESENTE' if c in df.columns else 'AUSENTE'}")

print("\n--- Amostra: mesmo cliente, transações diferentes ---")
primeiro_hash = df.select("cpf_pagador_hash").first()["cpf_pagador_hash"]
(
    df.filter(df.cpf_pagador_hash == primeiro_hash)
    .select("transacao_id", "valor", "qtd_transacoes_cliente", "valor_medio_cliente")
    .show(5, truncate=False)
)

print("\n--- quality_report.json: seção bivariada ---")
with open(f"{lake_path}/quality_reports/quality_report_{data_ref}.json") as f:
    report = json.load(f)
print(json.dumps(report.get("bivariada", "CHAVE 'bivariada' NAO ENCONTRADA"), indent=2, ensure_ascii=False))

print("\n--- Gold: metricas_por_cliente ---")
try:
    df_gold_cliente = spark.read.parquet(f"{lake_path}/gold/metricas_por_cliente").filter(
        f"data_ref = '{data_ref}'"
    )
    total_clientes = df_gold_cliente.count()
    print(f"  Total de linhas (deve bater com o total de clientes cadastrados): {total_clientes}")
    print("\n  Top 5 clientes por taxa de suspeita:")
    df_gold_cliente.orderBy(df_gold_cliente.taxa_suspeita.desc()).show(5, truncate=False)
except Exception as e:
    print(f"  TABELA NAO ENCONTRADA OU ERRO AO LER: {e}")

spark.stop()

#docker compose exec --user root spark-master python3 /opt/spark_jobs/verificar_novas_metricas.py 2026-09-19