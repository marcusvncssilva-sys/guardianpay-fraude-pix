"""
Data Quality Framework — GuardianPay
=====================================
Framework de validação de qualidade de dados implementado do zero em
PySpark (sem uso de Great Expectations, Soda ou similares, conforme
restrição do Projeto Final).

Checks implementados:
    - completude          (% de não-nulos em colunas críticas)
    - unicidade            (% de registros com chave única)
    - integridade referencial (% de FKs que existem na tabela de referência)
    - validade de domínio  (regras de negócio como expressões SQL)

Uso típico:
    dq = DataQualityFramework(spark)
    dq.check_completeness(df, ["cpf_pagador", "valor", "timestamp"], threshold=0.98)
    dq.check_uniqueness(df, ["transacao_id"])
    dq.check_validity(df, {"valor_positivo": "valor > 0"})
    df_valid, df_quarentena = dq.quarantine(df, {"valor_ok": "valor > 0"})
    report = dq.generate_report()
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)
    severity: str = "critical"  # "critical" | "warning" | "info"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "metric_value": round(self.metric_value, 4),
            "threshold": self.threshold,
            "details": self.details,
            "severity": self.severity,
        }


class DataQualityFramework:
    """Framework reutilizável de checks de qualidade de dados sobre DataFrames Spark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._results: List[CheckResult] = []

    # ------------------------------------------------------------------
    # Checks individuais
    # ------------------------------------------------------------------
    def check_completeness(
        self, df: DataFrame, columns: List[str], threshold: float = 0.95,
        severity: str = "critical",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            result = CheckResult("completeness", False, 0.0, threshold,
                                  {"motivo": "dataframe vazio"}, severity)
            self._results.append(result)
            return result

        taxas = {}
        for c in columns:
            nao_nulos = df.filter(F.col(c).isNotNull()).count()
            taxas[c] = nao_nulos / total

        pior_coluna = min(taxas, key=taxas.get)
        metric_value = taxas[pior_coluna]

        result = CheckResult(
            check_name=f"completude_{'_'.join(columns)}",
            passed=metric_value >= threshold,
            metric_value=metric_value,
            threshold=threshold,
            details={"taxas_por_coluna": taxas, "pior_coluna": pior_coluna, "total_registros": total},
            severity=severity,
        )
        self._results.append(result)
        return result

    def check_uniqueness(
        self, df: DataFrame, key_columns: List[str], severity: str = "critical",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            result = CheckResult("uniqueness", False, 0.0, 1.0,
                                  {"motivo": "dataframe vazio"}, severity)
            self._results.append(result)
            return result

        distintos = df.select(*key_columns).dropDuplicates().count()
        metric_value = distintos / total

        result = CheckResult(
            check_name=f"unicidade_{'_'.join(key_columns)}",
            passed=metric_value >= 0.999,  # tolera erro de arredondamento apenas
            metric_value=metric_value,
            threshold=0.999,
            details={"total_registros": total, "registros_distintos": distintos,
                     "duplicatas": total - distintos},
            severity=severity,
        )
        self._results.append(result)
        return result

    def check_referential_integrity(
        self, df_source: DataFrame, df_reference: DataFrame,
        source_col: str, ref_col: str, threshold: float = 0.95,
        severity: str = "warning",
    ) -> CheckResult:
        total = df_source.count()
        if total == 0:
            result = CheckResult("referential_integrity", False, 0.0, threshold,
                                  {"motivo": "dataframe vazio"}, severity)
            self._results.append(result)
            return result

        refs_validas = df_reference.select(F.col(ref_col).alias("_ref")).dropDuplicates()
        com_ref = (
            df_source.join(refs_validas, df_source[source_col] == refs_validas["_ref"], "left")
            .filter(F.col("_ref").isNotNull())
            .count()
        )
        metric_value = com_ref / total

        result = CheckResult(
            check_name=f"integridade_referencial_{source_col}",
            passed=metric_value >= threshold,
            metric_value=metric_value,
            threshold=threshold,
            details={"total_registros": total, "com_referencia_valida": com_ref},
            severity=severity,
        )
        self._results.append(result)
        return result

    def check_validity(
        self, df: DataFrame, rules: Dict[str, str], threshold: float = 0.95,
        severity: str = "critical",
    ) -> CheckResult:
        total = df.count()
        if total == 0:
            result = CheckResult("validity", False, 0.0, threshold,
                                  {"motivo": "dataframe vazio"}, severity)
            self._results.append(result)
            return result

        condicao_geral = " AND ".join(f"({expr})" for expr in rules.values())
        validos = df.filter(condicao_geral).count()
        metric_value = validos / total

        detalhes_por_regra = {}
        for nome, expr in rules.items():
            validos_regra = df.filter(expr).count()
            detalhes_por_regra[nome] = round(validos_regra / total, 4)

        result = CheckResult(
            check_name=f"validade_{'_'.join(rules.keys())}",
            passed=metric_value >= threshold,
            metric_value=metric_value,
            threshold=threshold,
            details={"taxa_por_regra": detalhes_por_regra, "total_registros": total},
            severity=severity,
        )
        self._results.append(result)
        return result

    # ------------------------------------------------------------------
    # Quarentena
    # ------------------------------------------------------------------
    def quarantine(
        self, df: DataFrame, rules: Dict[str, str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Separa df em (df_valido, df_quarentena) segundo as regras informadas.
        df_quarentena ganha a coluna 'quarentena_motivos' com os nomes das
        regras que o registro violou.
        """
        df_marcado = df
        for nome, expr in rules.items():
            df_marcado = df_marcado.withColumn(
                f"_falhou_{nome}", ~F.expr(expr) | F.expr(expr).isNull()
            )

        colunas_falha = [f"_falhou_{nome}" for nome in rules.keys()]

        df_marcado = df_marcado.withColumn(
            "quarentena_motivos",
            F.concat_ws(
                ",",
                *[F.when(F.col(c), F.lit(nome)) for nome, c in zip(rules.keys(), colunas_falha)],
            ),
        )
        df_marcado = df_marcado.withColumn(
            "_em_quarentena",
            F.array_max(F.array(*[F.col(c).cast("int") for c in colunas_falha])) == 1,
        )

        df_valido = df_marcado.filter(~F.col("_em_quarentena")).drop(
            *colunas_falha, "_em_quarentena", "quarentena_motivos"
        )
        df_quarentena = (
            df_marcado.filter(F.col("_em_quarentena"))
            .drop(*colunas_falha, "_em_quarentena")
            .withColumn("quarentena_ts", F.current_timestamp())
        )

        return df_valido, df_quarentena

    # ------------------------------------------------------------------
    # Execução em batch + relatório
    # ------------------------------------------------------------------
    def run_all_checks(self, df: DataFrame, config: Dict[str, Any]) -> List[CheckResult]:
        resultados = []
        if "completeness" in config:
            c = config["completeness"]
            resultados.append(self.check_completeness(df, c["columns"], c.get("threshold", 0.95)))
        if "uniqueness" in config:
            c = config["uniqueness"]
            resultados.append(self.check_uniqueness(df, c["key_columns"]))
        if "validity" in config:
            c = config["validity"]
            resultados.append(self.check_validity(df, c["rules"], c.get("threshold", 0.95)))
        if "referential_integrity" in config:
            c = config["referential_integrity"]
            resultados.append(self.check_referential_integrity(
                df, c["df_reference"], c["source_col"], c["ref_col"], c.get("threshold", 0.95)
            ))
        return resultados

    def generate_report(self) -> Dict[str, Any]:
        checks_total = len(self._results)
        checks_passed = sum(1 for r in self._results if r.passed)
        checks_failed = checks_total - checks_passed

        # O quality gate falha apenas se algum check "critical" falhar.
        # Checks "warning" não bloqueiam o pipeline, apenas geram alerta.
        gate_passed = all(r.passed for r in self._results if r.severity == "critical")

        overall_score = (
            sum(r.metric_value for r in self._results) / checks_total if checks_total else 0.0
        )

        return {
            "gerado_em": datetime.now(timezone.utc).isoformat(),
            "checks_total": checks_total,
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
            "gate_passed": gate_passed,
            "overall_score": round(overall_score, 4),
            "results": [r.to_dict() for r in self._results],
        }

    def reset(self):
        self._results = []
