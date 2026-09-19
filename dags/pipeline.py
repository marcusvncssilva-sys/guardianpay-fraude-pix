"""
DAG: Pipeline de Deteccao de Fraude PIX — GuardianPay
=======================================================
Projeto Final — Big Data Processing (MBA Engenharia de Dados, Mackenzie)

Fluxo:
    FileSensor (aguarda transacoes_pix.csv)
        -> SparkSubmit: ingestao.py       (Bronze)
        -> SparkSubmit: transformacao.py  (Silver + Quality Gate + Quarentena)
        -> PythonOperator: checar_quality_gate  (le o relatorio e decide seguir/alertar)
        -> SparkSubmit: agregacao.py      (Gold)
        -> PythonOperator: notificar_conclusao

Schedule: diario as 06:00 UTC
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# Configuracao geral
# ---------------------------------------------------------------------------
LAKE_PATH = "/opt/data/lake"
RAW_PATH = "/opt/data/raw"
SPARK_JOBS_PATH = "/opt/spark_jobs"
SPARK_MASTER = "spark://spark-master:7077"


def alerta_falha(context):
    task = context["task_instance"]
    print(f"[ALERTA] Task '{task.task_id}' falhou na DAG '{context['dag'].dag_id}'")
    print(f"  Data ref: {context['ds']}")
    print(f"  Erro: {context.get('exception', 'N/A')}")


default_args = {
    "owner": "guardianpay-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": alerta_falha,
}


# ---------------------------------------------------------------------------
# Funcoes Python
# ---------------------------------------------------------------------------
def checar_quality_gate(**context):
    """
    Le o relatorio de qualidade gerado pelo job Silver e decide se o
    pipeline pode seguir para a Gold. Se o gate critico falhou, a task
    (e a DAG) falha aqui — a Gold nunca roda sobre dado nao confiavel.
    """
    data_ref = context["ds"]
    report_path = f"{LAKE_PATH}/quality_reports/quality_report_{data_ref}.json"

    if not os.path.exists(report_path):
        raise FileNotFoundError(f"Relatorio de qualidade nao encontrado: {report_path}")

    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    print(f"[QUALITY] Relatorio: {report['checks_passed']}/{report['checks_total']} checks OK "
          f"| score={report['overall_score']} | gate_passed={report['gate_passed']}")

    for r in report["results"]:
        status = "OK" if r["passed"] else "FALHOU"
        print(f"   - {r['check_name']}: {status} (valor={r['metric_value']}, "
              f"threshold={r['threshold']}, severidade={r['severity']})")

    if not report["gate_passed"]:
        raise ValueError("Quality gate CRITICO falhou — pipeline interrompido antes da Gold.")

    context["ti"].xcom_push(key="quality_report", value=report)
    return report


def notificar_conclusao(**context):
    data_ref = context["ds"]
    report = context["ti"].xcom_pull(task_ids="checar_quality_gate", key="quality_report")

    print("=" * 60)
    print("PIPELINE GUARDIANPAY — DETECCAO DE FRAUDE PIX")
    print("=" * 60)
    print(f"  Data de referencia : {data_ref}")
    print(f"  Fluxo              : raw -> Bronze -> Silver -> Gold")
    print(f"  Quality gate       : PASSOU")
    if report:
        print(f"  Score de qualidade : {report['overall_score']}")
    print(f"  Tabelas Gold geradas:")
    print(f"    - gold/metricas_por_uf_mes")
    print(f"    - gold/ranking_risco_ispb")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="guardianpay_pipeline_fraude_pix",
    default_args=default_args,
    description="Pipeline E2E: Sensor -> Spark (Bronze/Silver/Gold) -> Quality Gate -> Notificacao",
    schedule="0 6 * * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=["guardianpay", "producao", "spark", "fraude", "projeto-final"],
) as dag:

    aguardar_arquivo = FileSensor(
        task_id="aguardar_arquivo_transacoes",
        filepath=f"{RAW_PATH}/transacoes/transacoes_pix.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
    )

    ingestao_bronze = SparkSubmitOperator(
        task_id="ingestao_bronze",
        application=f"{SPARK_JOBS_PATH}/ingestao.py",
        conn_id="spark_default",
        application_args=[
            "--data-ref", "{{ ds }}",
            "--input-path", RAW_PATH,
            "--output-path", LAKE_PATH,
        ],
        conf={"spark.master": SPARK_MASTER},
        verbose=False,
    )

    transformacao_silver = SparkSubmitOperator(
        task_id="transformacao_silver",
        application=f"{SPARK_JOBS_PATH}/transformacao.py",
        conn_id="spark_default",
        application_args=[
            "--data-ref", "{{ ds }}",
            "--lake-path", LAKE_PATH,
            "--report-path", f"{LAKE_PATH}/quality_reports",
        ],
        conf={"spark.master": SPARK_MASTER},
        verbose=False,
    )

    checar_quality_gate_task = PythonOperator(
        task_id="checar_quality_gate",
        python_callable=checar_quality_gate,
    )

    agregacao_gold = SparkSubmitOperator(
        task_id="agregacao_gold",
        application=f"{SPARK_JOBS_PATH}/agregacao.py",
        conn_id="spark_default",
        application_args=[
            "--data-ref", "{{ ds }}",
            "--lake-path", LAKE_PATH,
        ],
        conf={"spark.master": SPARK_MASTER},
        verbose=False,
    )

    notificar = PythonOperator(
        task_id="notificar_conclusao",
        python_callable=notificar_conclusao,
    )

    (
        aguardar_arquivo
        >> ingestao_bronze
        >> transformacao_silver
        >> checar_quality_gate_task
        >> agregacao_gold
        >> notificar
    )
