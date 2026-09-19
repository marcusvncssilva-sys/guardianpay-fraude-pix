# GuardianPay — Pipeline de Detecção de Fraude em Transações PIX

**Projeto Final — Big Data Processing**
MBA em Engenharia de Dados — Universidade Mackenzie
Opção B — Domínio Livre (Finanças / Detecção de Fraude)

**Integrante(s):** Marcus <!-- adicionar RA e demais integrantes, se houver -->

---

## O problema

A GuardianPay (fintech fictícia) processa transações PIX e precisa
identificar padrões suspeitos — valores altos em horários atípicos,
transações para instituições recebedoras de alto risco — além de gerar
métricas de negócio para compliance e diretoria. Este projeto entrega um
pipeline de dados completo, containerizado e orquestrado, que processa as
transações diariamente seguindo a arquitetura Medallion (Bronze → Silver
→ Gold), com quality gate e quarentena de dados inválidos.

Todos os dados são **sintéticos** (gerados por `gerar_dados.py`, via
Faker) — nenhum dado real de clientes ou de instituições financeiras é
utilizado.

Arquitetura detalhada e diagrama: [`docs/arquitetura.md`](docs/arquitetura.md)

## Como rodar

### Opção 1 — Docker Compose

```bash
docker compose up -d
```

Isso vai:
1. Gerar os dados sintéticos automaticamente (serviço `data-setup`)
2. Subir Spark (master + worker)
3. Subir Airflow (webserver + scheduler), com a DAG `guardianpay_pipeline_fraude_pix` já carregada

Acesse:
- Airflow UI: http://localhost:8081 (login `admin` / `admin`) — dispare a DAG manualmente (trigger) ou aguarde o schedule
- Spark Master UI: http://localhost:8080

```bash
docker compose down -v   # derruba tudo e limpa os volumes
```

### Opção 1b — GitHub Codespaces

Se você quer apresentar sem depender da RAM/CPU/rede da sua máquina no
dia, rode exatamente o mesmo `docker compose up` dentro de um Codespace
(VM do GitHub, acessada pelo navegador):

1. No GitHub, no seu repositório: **Code → Codespaces → Create codespace on main**
2. Aguarde o build (o `.devcontainer/devcontainer.json` já configura Docker-in-Docker, 4 cores e 8GB)
3. No terminal do Codespace: `docker compose up -d`
4. As portas 8080 (Spark) e 8081 (Airflow) são expostas automaticamente — o VS Code no navegador mostra um link clicável para cada uma

> **Atenção:** teste isso **com antecedência** (não no dia da apresentação).
> A conta gratuita do GitHub tem limite de horas de Codespaces por mês, e
> o primeiro build da imagem demora alguns minutos. Tenha o Plano B
> (`run_pipeline_local.sh` + screenshots/vídeo) preparado de qualquer forma
> — é a própria recomendação do `PROJETO_FINAL.md`.
>
> Esta opção roda a mesma arquitetura localmente dentro da VM do
> Codespace (nenhum serviço gerenciado de nuvem é usado), preservando a
> restrição de "100% local" do projeto — diferente de tentar provisionar
> o pipeline no AWS Academy Learner Lab, que exigiria infraestrutura à
> parte (EC2/Terraform) não coberta pelo `infra/` do curso, cujo escopo
> é apenas Jupyter+PySpark das Aulas 1–4.

### Opção 2 — Local, sem Docker/Airflow (plano B)

Caso o Docker falhe no dia da apresentação:

```bash
pip install -r requirements.txt
./run_pipeline_local.sh 2024-06-01
```

Isso roda Bronze → Silver → Gold via `spark-submit` local, sem depender
do Airflow, e imprime o relatório de qualidade no final.

## Estrutura do repositório

```
projeto-final-fraude-pix/
├── README.md                    # este arquivo
├── docker-compose.yml           # sobe Spark + Airflow com um comando
├── .devcontainer/
│   └── devcontainer.json         # roda tudo em GitHub Codespaces, sem depender da sua máquina
├── run_pipeline_local.sh        # plano B: roda tudo sem Airflow
├── gerar_dados.py               # gera os dados sinteticos (transacoes, clientes, watchlist)
├── requirements.txt
├── dags/
│   └── pipeline.py              # DAG do Airflow (Sensor -> Spark -> Quality Gate -> Notificacao)
├── spark_jobs/
│   ├── ingestao.py               # Bronze
│   ├── transformacao.py          # Silver (normalizacao + quality + quarentena)
│   └── agregacao.py              # Gold
├── quality/
│   ├── checks.py                 # DataQualityFramework (checks customizados, sem GE/Soda)
│   └── lgpd.py                    # Pseudonimização (hash + mascaramento) de CPF
├── tests/
│   ├── test_checks.py             # Testes do DataQualityFramework
│   └── test_lgpd.py               # Testes da pseudonimização LGPD
├── data/
│   ├── raw/                      # dados de entrada (gerados por gerar_dados.py)
│   └── lake/                     # bronze/, silver/, quarentena/, gold/, quality_reports/
└── docs/
    └── arquitetura.md            # diagrama e explicacao da arquitetura
```

## Fontes de dados

| Fonte | Formato | Descrição |
|---|---|---|
| `transacoes_pix.csv` | CSV | ~60.000 transações PIX sintéticas |
| `clientes.parquet` | Parquet | ~5.000 clientes (CPF, UF, segmento) |
| `ispb_watchlist.json` | JSON | Lista de referência de ISPBs com nível de risco |

## Camadas do Data Lake

- **Bronze**: dados brutos + `_source`, `_ingestion_ts`, `_ingestion_batch`
- **Silver**: normalizado, deduplicado, validado (quality gate) e enriquecido
- **Quarentena**: registros reprovados nos checks, com motivo
- **Gold**: `metricas_por_uf_mes` e `ranking_risco_ispb`

## Quality checks

4 checks customizados (completude, unicidade, validade de domínio,
integridade referencial) implementados do zero em `quality/checks.py` —
sem uso de Great Expectations ou Soda, conforme exigido pelo projeto.
Exemplo real de registro capturado pela quarentena, e a justificativa das
decisões de arquitetura (Parquet+SQLite, correção do `inferSchema`), estão
documentados em [`docs/arquitetura.md`](docs/arquitetura.md).

## LGPD

CPF de pagador e de recebedor são pseudonimizados (SHA-256 + mascaramento,
`quality/lgpd.py`) na transição Bronze → Silver — a Silver e a Gold nunca
contêm CPF em texto claro. Detalhes, ressalvas e mapeamento completo de
dados sensíveis em [`docs/arquitetura.md`](docs/arquitetura.md#conformidade-com-a-lgpd-e-segurança-da-informação).

## Stack

Python 3.11 · PySpark 3.5.3 · Apache Airflow 2.8.4 · Docker Compose 2.x

## Limitações conhecidas / próximos passos

- O gerador de CPF/ISPB não valida dígito verificador (não é necessário
  para o objetivo do projeto, apenas o formato de 11/8 dígitos é validado)
- A detecção de suspeita é uma regra simples (valor + horário + ISPB de
  risco) para fins didáticos — em produção seria um modelo de ML
- Airflow roda com `SequentialExecutor`/SQLite (adequado para o ambiente
  de laboratório de 8GB RAM; em produção usaria `LocalExecutor` + Postgres)
