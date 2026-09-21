"""
Gerador de Dados Sintéticos — GuardianPay (fintech fictícia)
=============================================================
Gera datasets sintéticos para o pipeline de detecção de fraude em
transações PIX. Nenhum dado real é utilizado, todos os valores
(CPFs, nomes, ISPBs) são gerados artificialmente com a lib Faker.

Saídas:
  data/raw/transacoes/transacoes_pix.csv   (~60.000 registros)
  data/raw/clientes/clientes.parquet
  data/raw/referencia/ispb_watchlist.json

Uso:
    python gerar_dados.py
    python gerar_dados.py --n-transacoes 60000 --n-clientes 5000 --seed 42
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


UFS = [
    "SP", "RJ", "MG", "RS", "PR", "BA", "SC", "GO", "PE", "CE",
    "PA", "DF", "ES", "MA", "MT", "MS", "AM", "PB", "RN", "AL",
]

CANAIS = ["app", "internet_banking", "caixa_eletronico", "whatsapp_pay", "api_terceiros"]
TIPOS_CHAVE = ["cpf", "email", "telefone", "aleatoria", "cnpj"]
SEGMENTOS_CLIENTE = ["varejo", "pessoa_fisica_premium", "pequena_empresa", "correntista_novo"]


def gerar_cpf_fake(fake: Faker) -> str:
    """Gera uma string de 11 dígitos no formato de CPF (sem validação de dígito verificador)."""
    return "".join(str(random.randint(0, 9)) for _ in range(11))


def gerar_ispb_fake() -> str:
    """ISPB tem 8 dígitos numéricos."""
    return "".join(str(random.randint(0, 9)) for _ in range(8))


def gerar_clientes(fake: Faker, n_clientes: int) -> pd.DataFrame:
    registros = []
    for i in range(n_clientes):
        cpf = gerar_cpf_fake(fake)
        data_cadastro = fake.date_between(start_date="-5y", end_date="-30d")
        registros.append({
            "cliente_id": f"CLI{i:07d}",
            "cpf": cpf,
            "nome": fake.name(),
            "uf": random.choice(UFS),
            "segmento": random.choices(
                SEGMENTOS_CLIENTE, weights=[0.55, 0.15, 0.10, 0.20]
            )[0],
            "data_cadastro": data_cadastro.isoformat(),
            "score_risco_cadastral": round(random.uniform(0, 1), 3),
        })
    df = pd.DataFrame(registros)
    # Injeta alguns nulos propositalmente (para exercitar os quality checks)
    idx_nulos = df.sample(frac=0.01, random_state=1).index
    df.loc[idx_nulos, "uf"] = None
    return df


def gerar_watchlist_ispb(n_ispbs: int = 40) -> dict:
    """Gera uma lista de referência de ISPBs com nível de risco associado."""
    ispbs = []
    for _ in range(n_ispbs):
        ispbs.append({
            "ispb": gerar_ispb_fake(),
            "nome_instituicao": f"Instituicao Financeira {random.randint(1, 999)}",
            "nivel_risco": random.choices(
                ["baixo", "medio", "alto"], weights=[0.7, 0.22, 0.08]
            )[0],
        })
    return {"gerado_em": datetime.utcnow().isoformat(), "ispbs_monitorados": ispbs}


def gerar_transacoes(
    fake: Faker, df_clientes: pd.DataFrame, watchlist: dict, n_transacoes: int
) -> pd.DataFrame:
    cpfs_clientes = df_clientes["cpf"].tolist()
    ispbs_watchlist = [i["ispb"] for i in watchlist["ispbs_monitorados"]]
    ispbs_risco_alto = {
        i["ispb"] for i in watchlist["ispbs_monitorados"] if i["nivel_risco"] == "alto"
    }

    data_inicio = datetime(2024, 1, 1)
    registros = []

    for i in range(n_transacoes):
        # Timestamp distribuído ao longo de 6 meses, com concentração maior em horário comercial
        dias_offset = random.randint(0, 179)
        hora = random.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 8, 7, 7, 7, 7, 6, 6, 5, 4, 3, 2, 1, 1],
        )[0]
        ts = data_inicio + timedelta(days=dias_offset, hours=hora, minutes=random.randint(0, 59))

        cpf_pagador = random.choice(cpfs_clientes)
        # 3% das transações são para o próprio CPF (transferência entre contas) — ignorar
        cpf_recebedor = gerar_cpf_fake(fake)

        ispb_recebedor = random.choice(ispbs_watchlist) if random.random() < 0.6 else gerar_ispb_fake()

        # Valores: maioria baixo valor, cauda longa com valores altos (padrão comum em fraude)
        valor = round(random.lognormvariate(4.5, 1.3), 2)
        valor = min(valor, 50000.0)

        # Sinaliza padrão suspeito: valor muito alto + horário de madrugada + ISPB de risco alto
        suspeita = (
            (valor > 5000 and hora in (0, 1, 2, 3, 4))
            or (ispb_recebedor in ispbs_risco_alto and valor > 2000)
        )

        registros.append({
            "transacao_id": str(uuid.uuid4()),
            "cpf_pagador": cpf_pagador,
            "cpf_recebedor": cpf_recebedor,
            "valor": valor,
            "timestamp": ts.isoformat(),
            "canal": random.choice(CANAIS),
            "tipo_chave_pix": random.choice(TIPOS_CHAVE),
            "ispb_recebedor": ispb_recebedor,
            "flag_suspeita_sintetica": suspeita,  # usado só para validação/apresentação, não é feature de produção
        })

    df = pd.DataFrame(registros)

    # Injeta problemas de qualidade propositalmente (~2% dos registros) para exercitar
    # os checks de completude, unicidade e validade de domínio
    n = len(df)
    idx_valor_nulo = df.sample(frac=0.005, random_state=2).index
    df.loc[idx_valor_nulo, "valor"] = None

    idx_valor_negativo = df.sample(frac=0.004, random_state=3).index
    df.loc[idx_valor_negativo, "valor"] = -abs(df.loc[idx_valor_negativo, "valor"].fillna(10.0))

    idx_cpf_nulo = df.sample(frac=0.003, random_state=4).index
    df.loc[idx_cpf_nulo, "cpf_pagador"] = None

    # Duplicatas propositais
    duplicatas = df.sample(frac=0.005, random_state=5)
    df = pd.concat([df, duplicatas], ignore_index=True)

    return df


def main():
    parser = argparse.ArgumentParser(description="Gera dados sintéticos GuardianPay")
    parser.add_argument("--n-transacoes", type=int, default=60000)
    parser.add_argument("--n-clientes", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-base", type=str, default="data/raw")
    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker("pt_BR")
    Faker.seed(args.seed)

    base = args.output_base
    os.makedirs(f"{base}/transacoes", exist_ok=True)
    os.makedirs(f"{base}/clientes", exist_ok=True)
    os.makedirs(f"{base}/referencia", exist_ok=True)

    print("[1/3] Gerando clientes...")
    df_clientes = gerar_clientes(fake, args.n_clientes)
    df_clientes.to_parquet(f"{base}/clientes/clientes.parquet", index=False)
    print(f"      -> {len(df_clientes)} clientes em {base}/clientes/clientes.parquet")

    print("[2/3] Gerando watchlist de ISPBs...")
    watchlist = gerar_watchlist_ispb()
    with open(f"{base}/referencia/ispb_watchlist.json", "w", encoding="utf-8") as f:
        json.dump(watchlist, f, ensure_ascii=False, indent=2)
    print(f"      -> {len(watchlist['ispbs_monitorados'])} ISPBs em {base}/referencia/ispb_watchlist.json")

    print("[3/3] Gerando transações PIX...")
    df_transacoes = gerar_transacoes(fake, df_clientes, watchlist, args.n_transacoes)
    df_transacoes.to_csv(f"{base}/transacoes/transacoes_pix.csv", index=False)
    print(f"      -> {len(df_transacoes)} transações em {base}/transacoes/transacoes_pix.csv")

    print("\nResumo:")
    print(f"  Transações: {len(df_transacoes)} (incl. duplicatas e problemas propositais)")
    print(f"  Clientes:   {len(df_clientes)}")
    print(f"  ISPBs:      {len(watchlist['ispbs_monitorados'])}")


if __name__ == "__main__":
    main()
