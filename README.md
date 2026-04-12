<p align="center">
  <img src="assets/Ferramenta digital futurista com estética cyberpunk.png" alt="WiredApply banner" width="980" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-MVP%20in%20progress-0F172A?style=for-the-badge" alt="status" />
  <img src="https://img.shields.io/badge/python-3.12+-0F172A?style=for-the-badge&logo=python&logoColor=FACC15" alt="python" />
  <img src="https://img.shields.io/badge/fastapi-api-0F172A?style=for-the-badge&logo=fastapi&logoColor=22D3EE" alt="fastapi" />
  <img src="https://img.shields.io/badge/postgres-asyncpg-0F172A?style=for-the-badge&logo=postgresql&logoColor=93C5FD" alt="postgres" />
  <img src="https://img.shields.io/badge/redis-cache-0F172A?style=for-the-badge&logo=redis&logoColor=F87171" alt="redis" />
  <img src="https://img.shields.io/badge/rabbitmq-events-0F172A?style=for-the-badge&logo=rabbitmq&logoColor=FB923C" alt="rabbitmq" />
  <img src="https://img.shields.io/badge/license-MIT-0F172A?style=for-the-badge" alt="license" />
</p>

<p align="center">
  <img src="https://skillicons.dev/icons?i=python,fastapi,postgres,redis,rabbitmq,docker,githubactions&perline=7" alt="tech stack" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=JetBrains+Mono&weight=500&size=16&duration=2500&pause=1000&color=9CA3AF&center=true&vCenter=true&width=980&lines=%5Bboot%5D+postgres+pool+online;%5Bboot%5D+redis+cache+%2B+locks+online;%5Bboot%5D+rabbitmq+channel+online;%5Brun%5D+scheduler+-%3E+pipeline.run+-%3E+workers+chain;%5Bguardrail%5D+final+submit+requires+human+confirmation" alt="runtime typing" />
</p>

<p align="center">
  <img src="assets/terminal-boot.svg" alt="Terminal boot preview" width="980" />
</p>

<p align="center">
  <img src="assets/wiredapply-ascii-animated.svg" alt="WiredApply ASCII animated terminal" width="980" />
</p>

<p align="center"><strong>PT-BR</strong> | <strong>EN</strong></p>

---

## PT-BR

### Visao geral

WiredApply e uma API open-source para transformar busca de vagas em operacao diaria.

- ingestao de vagas
- ranking por score
- candidaturas assistidas
- feedback do usuario para melhorar priorizacao
- digest diario

A proposta e simples: menos ruido, mais sinal, com controle humano no ponto critico.

### Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
psql -U postgres -d your_db -f schema.sql
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/openapi.json`

### Docker

```bash
copy .env.example .env
docker compose up --build -d
```

Com isso, API + PostgreSQL + Redis + RabbitMQ sobem juntos.
O `schema.sql` e aplicado automaticamente no primeiro boot do Postgres.

Para subir tambem os workers da pipeline:

```bash
docker compose --profile workers up --build -d
```

Para derrubar tudo:

```bash
docker compose down
```

Smoke test rapido da pipeline (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\pipeline_smoke_test.ps1
```

Worker SMTP (opcional):

```bash
python -m workers.smtp.email_worker
```

### Mapa rapido da API

| Modulo | Endpoint base | Papel |
|---|---|---|
| Auth | `/auth/*` | sessao, login e reset |
| Users | `/users/*` | perfil e conta |
| Pipeline | `/pipeline/*` | trigger e status do fluxo |
| Jobs | `/jobs/*` | CRUD de vagas |
| Applications | `/applications/*` | ciclo de candidatura |
| Feedback | `/feedback/*` | sinal para aprendizado |
| Digest | `/digest/*` | resumo diario |

### Pipeline de Workers

<p align="center">
  <img src="assets/pipeline-workers.svg" alt="Pipeline de Workers" width="1000" />
</p>

Detalhe por etapa:

| Queue | Worker consumidor | Saida principal |
|---|---|---|
| `ingestion.jobs` | `ingestion_worker` | vagas coletadas para `jobs.normalized` |
| `jobs.normalized` | `normalize_dedupe_worker` | vagas limpas + dedupe + envio para `scoring.jobs` |
| `scoring.jobs` | `scoring_worker` | score salvo + shortlist em `shortlist.apply` |
| `shortlist.apply` | `apply_worker` | atualiza `applications` |
| `retry.apply` | `retry_worker` | reprocessa falha tecnica com backoff |
| `digest.email` | `digest_worker` | prepara resumo diario para envio |

Pipeline status (MVP target):

<p>
  <img src="https://img.shields.io/badge/ingestion.jobs-worker%20chain-1F2937?style=flat-square" alt="ingestion badge" />
  <img src="https://img.shields.io/badge/jobs.normalized-dedupe%20stage-1F2937?style=flat-square" alt="normalized badge" />
  <img src="https://img.shields.io/badge/scoring.jobs-ranking%20stage-1F2937?style=flat-square" alt="scoring badge" />
  <img src="https://img.shields.io/badge/shortlist.apply-apply%20stage-1F2937?style=flat-square" alt="apply badge" />
  <img src="https://img.shields.io/badge/retry.apply-backoff%20stage-1F2937?style=flat-square" alt="retry badge" />
  <img src="https://img.shields.io/badge/digest.email-notify%20stage-1F2937?style=flat-square" alt="digest badge" />
</p>

### Snapshot de terminal

<p align="center">
  <img src="assets/terminal-run.svg" alt="Terminal runtime preview" width="980" />
</p>

### Roadmap curto

- applications CRUD completo com ownership estrito
- feedback CRUD com ajuste adaptativo de pesos
- ranking diario por score
- digest diario com envio por fila
- workers com idempotencia e retry

### Contribuindo

```text
1) Fork
2) Branch: feat/nome-curto
3) Commits pequenos e objetivos
4) Pull Request com contexto claro
```

### Seguranca

- nao versione segredos
- use `.env`
- valide ownership (`user_id`) em toda query de usuario
- mantenha confirmacao humana para submit final no MVP

### Licenca

MIT. Veja `LICENSE`.

---

## EN

### Overview

WiredApply is an open-source API for daily job-search operations.

- job ingestion
- score-based ranking
- assisted applications
- feedback-driven tuning
- digest delivery

Core idea: less noise, more signal, with human control at the final submit step.

### Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
psql -U postgres -d your_db -f schema.sql
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/openapi.json`

### Docker

```bash
copy .env.example .env
docker compose up --build -d
```

This starts API + PostgreSQL + Redis + RabbitMQ together.
`schema.sql` is applied automatically on the first Postgres boot.

To start the full worker chain too:

```bash
docker compose --profile workers up --build -d
```

To stop everything:

```bash
docker compose down
```

Quick pipeline smoke test (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\pipeline_smoke_test.ps1
```

SMTP worker (optional):

```bash
python -m workers.smtp.email_worker
```

### API map

| Module | Base endpoint | Role |
|---|---|---|
| Auth | `/auth/*` | session, login, reset |
| Users | `/users/*` | account and profile |
| Pipeline | `/pipeline/*` | trigger and status |
| Jobs | `/jobs/*` | jobs CRUD |
| Applications | `/applications/*` | application lifecycle |
| Feedback | `/feedback/*` | learning signal |
| Digest | `/digest/*` | daily summary |

### Workers pipeline

<p align="center">
  <img src="assets/pipeline-workers.svg" alt="Workers pipeline" width="1000" />
</p>

### Runtime snapshot

<p align="center">
  <img src="assets/terminal-run.svg" alt="Terminal runtime preview" width="980" />
</p>

### Short roadmap

- full applications CRUD with strict ownership checks
- feedback CRUD with adaptive weight updates
- daily ranking by score
- queue-based digest delivery
- idempotent workers with retry

### Contributing

```text
1) Fork
2) Branch: feat/short-name
3) Small, focused commits
4) Pull Request with clear context
```

### Security

- do not commit secrets
- use `.env`
- enforce ownership (`user_id`) in every user query
- keep human confirmation for final submit in MVP

### License

MIT. See `LICENSE`.
