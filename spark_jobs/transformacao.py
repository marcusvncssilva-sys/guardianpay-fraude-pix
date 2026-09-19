"""
Transformação — Camada Silver — GuardianPay
=============================================
Lê a camada Bronze, normaliza schema, trata nulls, remove duplicatas,
enriquece com clientes/watchlist e roda o Data Quality Framework.
Registros que falham nos checks críticos vão para quarentena.

Uso:
    python transformacao.py --data-ref 2024-06-01
"""

import argparse
import logging
import sys
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from quality.checks import DataQualityFramework  # noqa: E402
from quality.lgpd import aplicar_pseudonimizacao  # noqa: E402


def configurar_logging() -> logging.Logger:
    logger = logging.getLogger("transformacao_silver")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Transformação Silver — GuardianPay")
    parser.add_argument("--data-ref", type=str, required=True)
    parser.add_argument("--lake-path", type=str, default="data/lake")
    parser.add_argument("--report-path", type=str, default="data/lake/quality_reports")
    return parser.parse_args()


def main():
    args = parse_args()
    logger = configurar_logging()

    spark = (
        SparkSession.builder.appName(f"guardianpay-silver-{args.data_ref}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Iniciando transformação Silver | data_ref={args.data_ref}")

    df_transacoes = spark.read.parquet(f"{args.lake_path}/bronze/transacoes").filter(
        F.col("data_ref") == args.data_ref
    )
    df_clientes = spark.read.parquet(f"{args.lake_path}/bronze/clientes").filter(
        F.col("data_ref") == args.data_ref
    )
    df_watchlist = spark.read.parquet(f"{args.lake_path}/bronze/ispb_watchlist").filter(
        F.col("data_ref") == args.data_ref
    )

    n_bronze = df_transacoes.count()
    logger.info(f"  -> {n_bronze} transações lidas da Bronze")

    # ------------------------------------------------------------------
    # 1. Normalização de schema + tipos
    # ------------------------------------------------------------------
    df_transacoes_norm = (
        df_transacoes.withColumn("valor", F.col("valor").cast("double"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("cpf_pagador", F.trim(F.col("cpf_pagador")))
        .withColumn("ispb_recebedor", F.trim(F.col("ispb_recebedor")))
    )

    # ------------------------------------------------------------------
    # 2. Deduplicação (por transacao_id, mantendo o registro mais recente)
    # ------------------------------------------------------------------
    from pyspark.sql.window import Window

    janela = Window.partitionBy("transacao_id").orderBy(F.col("_ingestion_ts").desc())
    df_dedup = (
        df_transacoes_norm.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    n_apos_dedup = df_dedup.count()
    logger.info(f"  -> {n_apos_dedup} após deduplicação ({n_bronze - n_apos_dedup} duplicatas removidas)")

    # ------------------------------------------------------------------
    # 3. Quality checks (antes da quarentena, sobre os dados normalizados)
    # ------------------------------------------------------------------
    dq = DataQualityFramework(spark)

    dq.check_completeness(
        df_dedup, ["cpf_pagador", "valor", "timestamp", "ispb_recebedor"], threshold=0.98
    )
    dq.check_uniqueness(df_dedup, ["transacao_id"])
    dq.check_validity(
        df_dedup,
        {
            "valor_positivo": "valor > 0",
            "cpf_pagador_11_digitos": "length(cpf_pagador) = 11",
            "ispb_8_digitos": "length(ispb_recebedor) = 8",
            "timestamp_nao_futuro": "timestamp <= current_timestamp()",
        },
        threshold=0.90,
    )
    dq.check_referential_integrity(
        df_dedup, df_clientes, "cpf_pagador", "cpf", threshold=0.90
    )

    report = dq.generate_report()
    logger.info(
        f"Quality gate: {'PASSOU' if report['gate_passed'] else 'FALHOU'} "
        f"({report['checks_passed']}/{report['checks_total']} checks OK, score={report['overall_score']})"
    )

    os.makedirs(args.report_path, exist_ok=True)
    with open(f"{args.report_path}/quality_report_{args.data_ref}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # 4. Quarentena — separa válidos de inválidos
    # ------------------------------------------------------------------
    # NOTA: a chave de cada regra é o texto que aparece em quarentena_motivos
    # quando o registro REPROVA nela — por isso o nome descreve o problema
    # ("cpf_invalido_ou_ausente"), não a condição de sucesso. Nomear como
    # "cpf_valido" faria o relatório de quarentena dizer "cpf_valido" para
    # um registro justamente com CPF inválido, o que confunde quem lê.
    df_valido, df_quarentena = dq.quarantine(
        df_dedup,
        {
            "cpf_invalido_ou_ausente": "cpf_pagador IS NOT NULL AND length(cpf_pagador) = 11",
            "valor_invalido_ou_ausente": "valor IS NOT NULL AND valor > 0",
            "ispb_invalido_ou_ausente": "ispb_recebedor IS NOT NULL AND length(ispb_recebedor) = 8",
            "timestamp_invalido_ou_futuro": "timestamp IS NOT NULL AND timestamp <= current_timestamp()",
        },
    )

    n_valido = df_valido.count()
    n_quarentena = df_quarentena.count()
    logger.info(f"  -> {n_valido} válidos | {n_quarentena} em quarentena "
                f"({round(100 * n_quarentena / max(n_apos_dedup, 1), 2)}%)")

    # ------------------------------------------------------------------
    # 5. Enriquecimento com clientes e watchlist (apenas nos válidos)
    # ------------------------------------------------------------------
    df_watchlist_sel = df_watchlist.select(
        F.col("ispb").alias("_wl_ispb"), F.col("nivel_risco").alias("nivel_risco_ispb")
    )
    df_clientes_sel = df_clientes.select(
        F.col("cpf").alias("_cli_cpf"), F.col("uf"), F.col("segmento")
    )

    df_silver_enriquecido = (
        df_valido.join(df_watchlist_sel, df_valido.ispb_recebedor == df_watchlist_sel._wl_ispb, "left")
        .join(df_clientes_sel, df_valido.cpf_pagador == df_clientes_sel._cli_cpf, "left")
        .drop("_wl_ispb", "_cli_cpf")
        .fillna({"nivel_risco_ispb": "desconhecido", "uf": "NA", "segmento": "desconhecido"})
        .withColumn(
            "flag_suspeita",
            (F.col("nivel_risco_ispb") == "alto") & (F.col("valor") > 2000)
            | ((F.hour("timestamp").isin(0, 1, 2, 3, 4)) & (F.col("valor") > 5000)),
        )
    )

    # ------------------------------------------------------------------
    # 5b. LGPD — pseudonimização (aplicada SÓ AGORA, depois de todo o uso
    # do CPF em claro para joins/enriquecimento acima). cpf_pagador e
    # cpf_recebedor são ambos dados pessoais de titulares diferentes —
    # os dois recebem hash + mascaramento, e o texto claro é removido da
    # Silver. A Bronze e a Quarentena mantêm o valor original (acesso
    # restrito por design, ver docs/arquitetura.md).
    # ------------------------------------------------------------------
    df_silver = aplicar_pseudonimizacao(
        df_silver_enriquecido, colunas_cpf=["cpf_pagador", "cpf_recebedor"]
    ).drop("cpf_pagador", "cpf_recebedor")

    # ------------------------------------------------------------------
    # 6. Escrita idempotente
    # ------------------------------------------------------------------
    (
        df_silver.write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.lake_path}/silver/transacoes")
    )
    (
        df_quarentena.write.mode("overwrite")
        .partitionBy("data_ref")
        .parquet(f"{args.lake_path}/quarentena/transacoes")
    )

    logger.info("Transformação Silver concluída com sucesso.")

    if not report["gate_passed"]:
        logger.error("Quality gate CRÍTICO falhou — pipeline deve ser interrompido.")
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
