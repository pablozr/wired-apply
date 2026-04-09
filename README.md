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

```text
wiredapply@ops:~$ ./boot
[ok] postgres pool connected
[ok] redis connected
[ok] rabbitmq channel connected
[ok] api docs available at /docs
```

```text
__        ___              _      _                
\ \      / (_)_ __ ___  __| |    / \   _ __  _ __  
 \ \ /\ / /| | '__/ _ \/ _` |   / _ \ | '_ \| '_ \ 
  \ V  V / | | | |  __/ (_| |  / ___ \| |_) | |_) |
   \_/\_/  |_|_|  \___|\__,_| /_/   \_\ .__/| .__/ 
                                      |_|   |_|    
```

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

```text
[scheduler/manual]
       |
       v
POST /pipeline/run
       |
       v
  queue: ingestion.jobs
       |
       v
worker: ingestion_worker
       |
       v
  queue: jobs.normalized
       |
       v
worker: normalize_dedupe_worker
       |
       v
  queue: scoring.jobs
       |
       v
worker: scoring_worker
   |                     \
   |                      +--> queue: digest.email -> digest_worker
   v
queue: shortlist.apply -> apply_worker
                           |
                           +--> queue: retry.apply -> retry_worker -> shortlist.apply
```

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

```text
wiredapply@ops:~$ curl -X POST http://localhost:8000/pipeline/run -b "auth=..."
{"message":"Pipeline run queued","data":{"runId":"9b6b..."}}

wiredapply@ops:~$ curl http://localhost:8000/pipeline/status -b "auth=..."
{"message":"Pipeline status retrieved successfully","data":{"jobsCount":42,"applicationsCount":6}}
```

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

```text
scheduler/manual -> /pipeline/run -> ingestion.jobs
ingestion_worker -> jobs.normalized
normalize_dedupe_worker -> scoring.jobs
scoring_worker -> shortlist.apply + digest.email
apply_worker -> applications
retry_worker (retry.apply) -> shortlist.apply
digest_worker -> daily digest notification flow
```

### Runtime snapshot

```text
wiredapply@ops:~$ curl -X POST http://localhost:8000/pipeline/run -b "auth=..."
{"message":"Pipeline run queued","data":{"runId":"9b6b..."}}
```

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
