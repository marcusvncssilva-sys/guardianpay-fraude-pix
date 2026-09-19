"""
Ingestão — Camada Bronze — GuardianPay
========================================
Lê as 3 fontes de dados (formatos diferentes) e grava a camada Bronze
com metadados de ingestão (_source, _ingestion_ts, _ingestion_batch).

Fontes:
    - transacoes_pix.csv        (CSV)
    - clientes.parquet          (Parquet)
    - ispb_watchlist.json       (JSON)

Uso:
    python ingestao.py --data-ref 2024-06-01
    spark-submit ingestao.py --data-ref 2024-06-01 --input-path /data/raw --output-path /data/lake
"""

import argparse
import logging
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)


# Schema explícito para as transações: CPF e ISPB são identificadores com
# zeros à esquerda significativos — NUNCA usar inferSchema aqui, ou o Spark
# os interpreta como números e corrompe o dado (bug real, comum em produção).
SCHEMA_TRANSACOES = StructType([
    StructField("transacao_id", StringType(), True),
    StructField("cpf_pagador", StringType(), True),
    StructField("cpf_recebedor", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("tipo_chave_pix", StringType(), True),
    StructField("ispb_recebedor", StringType(), True),
    StructField("flag_suspeita_sintetica", BooleanType(), True),
])


def configurar_logging() -> logging.Logger:
    logger = logging.getLogger("ingestao_bronze")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão Bronze — GuardianPay")
    parser.add_argument("--data-ref", type=str, required=True, help="Data de referência YYYY-MM-DD")
    parser.add_argument("--input-path", type=str, default="data/raw")
    parser.add_argument("--output-path", type=str, default="data/lake")
    return parser.parse_args()


def adicionar_metadados(df, source: str, batch_id: str):
    return (
        df.withColumn("_source", F.lit(source))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )


def main():
    args = parse_args()
    logger = configurar_logging()
    batch_id = str(uuid.uuid4())

    spark = (
        SparkSession.builder.appName(f"guardianpay-bronze-{args.data_ref}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Iniciando ingestão Bronze | data_ref={args.data_ref} | batch_id={batch_id}")

    # ------------------------------------------------------------------
    # 1. Transações PIX (CSV)
    # ------------------------------------------------------------------
    logger.info("Lendo transacoes_pix.csv ...")
    df_transacoes = spark.read.option("header", True).schema(SCHEMA_TRANSACOES).csv(
        f"{args.input_path}/transacoes/transacoes_pix.csv"
    )
    df_transacoes_bronze = adicionar_metadados(df_transacoes, "transacoes_pix_csv", batch_id)
    n_transacoes = df_transacoes_bronze.count()
    logger.info(f"  -> {n_transacoes} registros lidos")

    # ------------------------------------------------------------------
    # 2. Clientes (Parquet)
    # ------------------------------------------------------------------
    logger.info("Lendo clientes.parquet ...")
    df_clientes = spark.read.parquet(f"{args.input_path}/clientes/clientes.parquet")
    df_clientes_bronze = adicionar_metadados(df_clientes, "clientes_parquet", batch_id)
    n_clientes = df_clientes_bronze.count()
    logger.info(f"  -> {n_clientes} registros lidos")

    # ------------------------------------------------------------------
    # 3. Watchlist de ISPBs (JSON)
    # ------------------------------------------------------------------
    logger.info("Lendo ispb_watchlist.json ...")
    df_watchlist_raw = spark.read.option("multiLine", True).json(
        f"{args.input_path}/referencia/ispb_watchlist.json"
    )
    df_watchlist = df_watchlist_raw.select(F.explode("ispbs_monitorados").alias("ispb_info")).select(
        "ispb_info.*"
    )
    df_watchlist_bronze = adicionar_metadados(df_watchlist, "ispb_watchlist_json", batch_id)
    n_watchlist = df_watchlist_bronze.count()
    logger.info(f"  -> {n_watchlist} registros lidos")

    # ------------------------------------------------------------------
    # Escrita idempotente: overwrite particionado por data_ref
    # ------------------------------------------------------------------
    logger.info("Gravando camada Bronze (Parquet, particionado por data_ref) ...")

    (
        df_transacoes_bronze.withColumn("data_ref", F.lit(args.data_ref))
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.output_path}/bronze/transacoes")
    )
    (
        df_clientes_bronze.withColumn("data_ref", F.lit(args.data_ref))
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.output_path}/bronze/clientes")
    )
    (
        df_watchlist_bronze.withColumn("data_ref", F.lit(args.data_ref))
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.output_path}/bronze/ispb_watchlist")
    )

    logger.info("Ingestão Bronze concluída com sucesso.")
    logger.info(f"Resumo: transacoes={n_transacoes} clientes={n_clientes} ispbs={n_watchlist}")

    spark.stop()


if __name__ == "__main__":
    main()