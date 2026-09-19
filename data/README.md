# data/

Esta pasta é populada automaticamente ao rodar `gerar_dados.py` (ou pelo
serviço `data-setup` do `docker compose up`). Os dados sintéticos não
são versionados no Git (ver `.gitignore`) — apenas os scripts que os geram.

```
data/
├── raw/         # gerado por gerar_dados.py (transacoes, clientes, referencia)
└── lake/        # gerado pelo pipeline: bronze/, silver/, quarentena/, gold/, quality_reports/
```
