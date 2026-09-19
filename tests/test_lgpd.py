"""
Testes unitários do módulo de pseudonimização LGPD (quality/lgpd.py).

Uso:
    pytest tests/test_lgpd.py -v
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from quality.lgpd import aplicar_pseudonimizacao  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("test-lgpd").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def df_cpfs(spark):
    dados = [
        ("T1", "12345678901", "98765432100"),
        ("T2", "12345678901", "11122233344"),  # mesmo pagador de T1
        ("T3", "55566677788", "98765432100"),  # mesmo recebedor de T1
    ]
    return spark.createDataFrame(dados, ["transacao_id", "cpf_pagador", "cpf_recebedor"])


def test_hash_e_determinístico_para_mesmo_cpf(spark, df_cpfs):
    df = aplicar_pseudonimizacao(df_cpfs, ["cpf_pagador"]).collect()
    hash_t1 = [r for r in df if r.transacao_id == "T1"][0].cpf_pagador_hash
    hash_t2 = [r for r in df if r.transacao_id == "T2"][0].cpf_pagador_hash
    hash_t3 = [r for r in df if r.transacao_id == "T3"][0].cpf_pagador_hash

    assert hash_t1 == hash_t2  # mesmo CPF pagador -> mesmo hash
    assert hash_t1 != hash_t3  # CPFs diferentes -> hashes diferentes


def test_hash_tem_64_caracteres_sha256(spark, df_cpfs):
    df = aplicar_pseudonimizacao(df_cpfs, ["cpf_pagador"]).collect()
    for row in df:
        assert len(row.cpf_pagador_hash) == 64


def test_mascara_no_formato_esperado(spark, df_cpfs):
    df = aplicar_pseudonimizacao(df_cpfs, ["cpf_pagador"]).collect()
    for row in df:
        assert row.cpf_pagador_mascarado.startswith(row.cpf_pagador[:3])
        assert row.cpf_pagador_mascarado.endswith(row.cpf_pagador[-2:])
        assert ".***.***-" in row.cpf_pagador_mascarado


def test_aplica_em_multiplas_colunas(spark, df_cpfs):
    df = aplicar_pseudonimizacao(df_cpfs, ["cpf_pagador", "cpf_recebedor"])
    colunas = df.columns
    for esperado in [
        "cpf_pagador_hash", "cpf_pagador_mascarado",
        "cpf_recebedor_hash", "cpf_recebedor_mascarado",
    ]:
        assert esperado in colunas


def test_colunas_originais_nao_sao_removidas_pela_funcao(spark, df_cpfs):
    # a função só adiciona colunas — dropar o texto claro é responsabilidade
    # de quem chama, e deve acontecer só depois de qualquer uso do CPF puro
    df = aplicar_pseudonimizacao(df_cpfs, ["cpf_pagador"])
    assert "cpf_pagador" in df.columns
