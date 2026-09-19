#!/usr/bin/env bash
# =============================================================================
# Plano B para a demo: roda o pipeline Bronze -> Silver -> Gold direto,
# sem depender do Airflow, caso algo dê errado com a UI no dia da apresentação.
#
# Uso:
#   ./run_pipeline_local.sh 2024-06-01
# =============================================================================
set -e

DATA_REF="${1:-2024-06-01}"

echo "=============================================="
echo " GuardianPay - Pipeline Fraude PIX (modo local)"
echo " data_ref=${DATA_REF}"
echo "=============================================="

if [ ! -f "data/raw/transacoes/transacoes_pix.csv" ]; then
    echo "[SETUP] Gerando dados sinteticos..."
    python3 gerar_dados.py --n-transacoes 60000 --n-clientes 5000 --seed 42
fi

echo "[1/3] Ingestao Bronze..."
python3 spark_jobs/ingestao.py --data-ref "${DATA_REF}" --input-path data/raw --output-path data/lake

echo "[2/3] Transformacao Silver + Quality Gate..."
python3 spark_jobs/transformacao.py --data-ref "${DATA_REF}" --lake-path data/lake

echo "[3/3] Agregacao Gold..."
python3 spark_jobs/agregacao.py --data-ref "${DATA_REF}" --lake-path data/lake

echo "=============================================="
echo " Pipeline concluido com sucesso!"
echo " Relatorio de qualidade: data/lake/quality_reports/quality_report_${DATA_REF}.json"
echo " Tabelas Gold: data/lake/gold/"
echo "=============================================="
