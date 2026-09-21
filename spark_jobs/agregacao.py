"""
Agregação — Camada Gold — GuardianPay
========================================
Lê a camada Silver (transações válidas e enriquecidas) e gera as
tabelas agregadas de negócio:

    gold/metricas_por_uf_mes        -> volume, valor total e taxa de suspeita por UF/mês
    gold/metricas_por_uf_mes        -> volume, valor total e taxa de suspeita por UF/mês
    gold/ranking_risco_ispb         -> ranking de ISPBs por indicadores de risco
    gold/metricas_por_cliente       -> volume, valor total e taxa de suspeita por cliente

Uso:
    gold/ranking_risco_ispb         -> ranking de ISPBs por indicadores de risco
    python agregacao.py --data-ref 2024-06-01
    
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def configurar_logging() -> logging.Logger:
    logger = logging.getLogger("agregacao_gold")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Agregação Gold — GuardianPay")
    parser.add_argument("--data-ref", type=str, required=True)
    parser.add_argument("--lake-path", type=str, default="data/lake")
    return parser.parse_args()


def main():
    args = parse_args()
    logger = configurar_logging()

    spark = (
        SparkSession.builder.appName(f"guardianpay-gold-{args.data_ref}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Iniciando agregação Gold | data_ref={args.data_ref}")

    df_silver = spark.read.parquet(f"{args.lake_path}/silver/transacoes").filter(
        F.col("data_ref") == args.data_ref
    )
    n = df_silver.count()
    logger.info(f"  -> {n} transações válidas lidas da Silver")

    df_silver = df_silver.withColumn("ano_mes", F.date_format("timestamp", "yyyy-MM"))

    # ------------------------------------------------------------------
    # Gold 1 — Métricas por UF e mês (visão executiva de volume/valor/risco)
    # ------------------------------------------------------------------
    df_metricas_uf_mes = (
        df_silver.groupBy("uf", "ano_mes")
        .agg(
            F.count("*").alias("qtd_transacoes"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("valor_medio"),
            F.sum(F.col("flag_suspeita").cast("int")).alias("qtd_suspeitas"),
        )
        .withColumn(
            "taxa_suspeita",
            F.round(F.col("qtd_suspeitas") / F.col("qtd_transacoes"), 4),
        )
        .withColumn("data_ref", F.lit(args.data_ref))
        .orderBy("ano_mes", "uf")
    )

    # ------------------------------------------------------------------
    # Gold 2 — Ranking de risco por ISPB recebedor
    # ------------------------------------------------------------------
    df_ranking_ispb = (
        df_silver.groupBy("ispb_recebedor", "nivel_risco_ispb")
        .agg(
            F.count("*").alias("qtd_transacoes"),
            F.sum("valor").alias("valor_total_recebido"),
            F.avg("valor").alias("valor_medio"),
            F.sum(F.col("flag_suspeita").cast("int")).alias("qtd_suspeitas"),
        )
        .withColumn(
            "taxa_suspeita",
            F.round(F.col("qtd_suspeitas") / F.col("qtd_transacoes"), 4),
        )
        .withColumn("data_ref", F.lit(args.data_ref))
        .orderBy(F.desc("qtd_suspeitas"))
    )

    (
        df_metricas_uf_mes.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.lake_path}/gold/metricas_por_uf_mes")
    )
    (
        df_ranking_ispb.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.lake_path}/gold/ranking_risco_ispb")
    )

    # ------------------------------------------------------------------
    # Gold 3 — Métricas por cliente (cpf_pagador_hash), nível de detalhe
    # mais próximo do usado em detecção de fraude real. Usa o hash já
    # pseudonimizado na Silver, nenhum CPF em texto claro chega até aqui.
    # ------------------------------------------------------------------
    df_metricas_cliente = (
        df_silver.groupBy("cpf_pagador_hash")
        .agg(
            F.count("*").alias("qtd_transacoes"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("valor_medio"),
            F.sum(F.col("flag_suspeita").cast("int")).alias("qtd_suspeitas"),
        )
        .withColumn(
            "taxa_suspeita",
            F.round(F.col("qtd_suspeitas") / F.col("qtd_transacoes"), 4),
        )
        .withColumn("data_ref", F.lit(args.data_ref))
        .orderBy(F.desc("taxa_suspeita"))
    )
    (
        df_metricas_cliente.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.lake_path}/gold/metricas_por_cliente")
    )

    logger.info("Amostra — metricas_por_uf_mes:")
    df_metricas_uf_mes.show(5, truncate=False)
    logger.info("Amostra — ranking_risco_ispb (top suspeitas):")
    df_ranking_ispb.show(5, truncate=False)
    logger.info("Amostra — metricas_por_cliente (top suspeitas):")
    df_metricas_cliente.show(5, truncate=False)

    logger.info("Agregação Gold concluída com sucesso.")
    spark.stop()


if __name__ == "__main__":
    main()