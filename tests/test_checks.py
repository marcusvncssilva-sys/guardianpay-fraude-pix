"""
Testes unitários do DataQualityFramework.

Uso:
    pytest tests/test_checks.py -v
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from quality.checks import DataQualityFramework  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("test-quality-framework")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def df_exemplo(spark):
    dados = [
        ("T1", "12345678901", 100.0),
        ("T2", "12345678902", 200.0),
        ("T3", None, 300.0),          # nulo em cpf
        ("T4", "12345678904", -50.0),  # valor negativo
        ("T1", "12345678901", 100.0),  # duplicata de T1
    ]
    return spark.createDataFrame(dados, ["transacao_id", "cpf_pagador", "valor"])


def test_completeness_detecta_nulos(spark, df_exemplo):
    dq = DataQualityFramework(spark)
    result = dq.check_completeness(df_exemplo, ["cpf_pagador"], threshold=0.95)
    assert result.metric_value == pytest.approx(4 / 5)
    assert result.passed is False


def test_uniqueness_detecta_duplicata(spark, df_exemplo):
    dq = DataQualityFramework(spark)
    result = dq.check_uniqueness(df_exemplo, ["transacao_id"])
    assert result.details["duplicatas"] == 1
    assert result.passed is False


def test_validity_valor_positivo(spark, df_exemplo):
    dq = DataQualityFramework(spark)
    result = dq.check_validity(df_exemplo, {"valor_positivo": "valor > 0"}, threshold=0.9)
    assert result.details["taxa_por_regra"]["valor_positivo"] == pytest.approx(4 / 5)


def test_quarantine_separa_invalidos(spark, df_exemplo):
    dq = DataQualityFramework(spark)
    df_valido, df_quarentena = dq.quarantine(
        df_exemplo, {"cpf_ok": "cpf_pagador IS NOT NULL", "valor_ok": "valor > 0"}
    )
    assert df_quarentena.count() == 2  # T3 (cpf nulo) e T4 (valor negativo)
    assert df_valido.count() == 3


def test_generate_report_gate(spark, df_exemplo):
    dq = DataQualityFramework(spark)
    dq.check_completeness(df_exemplo, ["cpf_pagador"], threshold=0.99, severity="critical")
    report = dq.generate_report()
    assert report["gate_passed"] is False
    assert report["checks_total"] == 1
