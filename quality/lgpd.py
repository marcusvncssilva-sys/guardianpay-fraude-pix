"""
LGPD — Pseudonimização e Mascaramento de Dados Pessoais
==========================================================
Implementa os controles de privacidade aplicados na transição
Bronze -> Silver do pipeline GuardianPay, alinhados ao princípio de
minimização de dados da LGPD (Lei 13.709/2018).

Não substitui uma avaliação jurídica formal — trata-se de um projeto
acadêmico com dados 100% sintéticos. O objetivo aqui é demonstrar a
técnica de engenharia (privacy by design), não emitir um parecer de
conformidade legal.

Uso:
    from quality.lgpd import aplicar_pseudonimizacao

    df_pseudo = aplicar_pseudonimizacao(df, colunas_cpf=["cpf_pagador", "cpf_recebedor"])
    df_final = df_pseudo.drop("cpf_pagador", "cpf_recebedor")  # remove o texto claro
"""

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LGPD_SALT_ENV_VAR = "LGPD_SALT_KEY"

# ATENÇÃO: este valor default é apenas para rodar o projeto localmente/na
# demo. Em produção real, o salt DEVE vir de um cofre de segredos (Vault,
# AWS Secrets Manager etc.) via variável de ambiente — nunca hardcoded.
_DEFAULT_SALT_DEMO = "guardianpay_demo_salt_nao_usar_em_producao"


def _obter_salt() -> str:
    return os.getenv(LGPD_SALT_ENV_VAR, _DEFAULT_SALT_DEMO)


def aplicar_pseudonimizacao(df: DataFrame, colunas_cpf: list) -> DataFrame:
    """
    Para cada coluna em `colunas_cpf`, adiciona duas novas colunas:
      - <coluna>_hash       : SHA-256(coluna + salt) — determinístico,
                              permite agrupar (GROUP BY) pelo mesmo titular
                              sem expor o CPF.
      - <coluna>_mascarado  : formato "123.***.***-01" para uso em logs,
                              dashboards e suporte operacional.

    As colunas originais NÃO são removidas por esta função — a decisão de
    dropar o texto claro é de quem chama (normalmente após todo enriquecimento
    e join que ainda precisem do valor original).
    """
    salt = _obter_salt()
    df_resultado = df
    for coluna in colunas_cpf:
        df_resultado = (
            df_resultado.withColumn(
                f"{coluna}_hash",
                F.sha2(F.concat(F.col(coluna), F.lit(salt)), 256),
            ).withColumn(
                f"{coluna}_mascarado",
                F.concat(
                    F.substring(F.col(coluna), 1, 3),
                    F.lit(".***.***-"),
                    F.substring(F.col(coluna), 10, 2),
                ),
            )
        )
    return df_resultado
