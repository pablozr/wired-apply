# Guia de implementacao Frontend (UX/UI + endpoints)

## 1) Objetivo

Este guia descreve como implementar o frontend do WiredApply com:

- fluxo UX/UI completo (usuario e admin)
- contrato de API mapeado endpoint por endpoint
- boas praticas de estado, polling e tratamento de erro

Foco: telas web (SPA) com consumo da API FastAPI.

## 2) Contrato base da API

### 2.1 Base URL e envelope de resposta

- Base URL local: `http://localhost:8000`
- OpenAPI: `/openapi.json`
- Docs: `/docs`

Sucesso (200/201):

```json
{
  "message": "...",
  "data": {"...": "..."}
}
```

Erro de negocio (400):

```json
{
  "detail": "mensagem de erro"
}
```

### 2.2 Autenticacao

- Login seta cookie `auth` (HttpOnly, SameSite=Lax, Secure=true).
- Frontend deve enviar `credentials: 'include'` nas chamadas autenticadas.
- Em browser, `Secure=true` exige HTTPS para cookie funcionar de ponta a ponta.
- API tambem aceita `Authorization: Bearer <token>` como fallback tecnico (principalmente para scripts/smoke).

### 2.3 Autorizacao

- Usuario comum: role `BASIC`
- Admin: role `ADMIN`
- Endpoints admin retornam `403 Insufficient permissions` para quem nao for admin.

### 2.4 Limites importantes

- `limit` em listagens: maximo 100
- Janela de data (`daysRange` ou `dateFrom/dateTo`): maximo 30 dias
- Auth com rate limit em:
  - `POST /auth/login`
  - `POST /auth/forget-password`
  - `POST /auth/validate-code`

## 3) Arquitetura UX/UI recomendada

### 3.1 Mapa de telas

- `/login`
- `/cadastro`
- `/esqueci-senha` (codigo + nova senha)
- `/app/dashboard` (resumo e status de pipeline)
- `/app/perfil` (objetivo, senioridade, preferencias)
- `/app/curriculo` (upload PDF + status de parse)
- `/app/ranking` (lista priorizada por score)
- `/app/vagas/:jobId` (detalhe + score e breakdown)
- `/app/aplicacoes` (funil de candidatura)
- `/app/feedback` (historico e envio de rating)
- `/app/digest` (resumo diario)
- `/app/admin/operacoes` (global ingestion + cleanup)

### 3.2 Padrao UX de fluxo principal

1. Usuario autentica.
2. Completa perfil e envia curriculo.
3. Dispara pipeline (`/pipeline/run`).
4. Front faz polling em `/pipeline/status` a cada 3-5s.
5. Ao concluir, frontend abre ranking (`/jobs/ranking/daily`).
6. Usuario aprofunda em vaga (`/jobs/{id}/score`) e registra aplicacao/feedback.

### 3.3 Componentes e estados de interface

- **Header status**: mostra `isRunning`, `activeRunId`, `lastRun.status`.
- **Cards de metricas**: `jobsProcessed`, `jobsFailed`, `aiCalls`, `aiCacheHitRate`, `aiPrefilterRejected`.
- **Tabela/lista de ranking**:
  - colunas: score, bucket, empresa, local, effectiveDate, aiScore, confidence.
  - cores por bucket: `A >= 80`, `B >= 60`, `C < 60`.
- **Drawer/modal detalhe vaga**: `reason`, `aiReason`, `aiBreakdown`, `aiSkippedReason`.
- **Estados obrigatorios**: loading, empty, error, stale-data.

### 3.4 Polling recomendado

- Iniciar polling apos `POST /pipeline/run`.
- Encerrar quando:
  - `lastRun.runId` for o run iniciado E
  - `lastRun.status` for `COMPLETED` (ou erro final tratado pela API/UX).
- Timeout sugerido: 10-15 min com mensagem amigavel e opcao "continuar monitorando".

## 4) Endpoints mapeados

### 4.1 Auth

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| POST | `/auth/login` | publica | `{ email, password }` | `{ user }` + cookie `auth` | Login |
| POST | `/auth/logout` | publica | - | `{}` | Logout |
| POST | `/auth/forget-password` | publica | `{ email }` | `{}` + cookie `auth_reset` | Inicio reset |
| POST | `/auth/validate-code` | cookie `auth_reset` | `{ code }` | `{}` + novo `auth_reset` | Validacao codigo |
| POST | `/auth/update-password` | cookie `auth_reset` com permissao | `{ password }` | `{ user }` | Finaliza reset |

### 4.2 Usuarios, perfil e curriculo

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| POST | `/users/` | publica | `{ fullName, email, password }` | `{ user }` | Cadastro |
| GET | `/users/me` | usuario logado | - | `{ user }` | Sessao atual |
| PUT | `/users/me` | usuario logado | `{ fullName?, email? }` | `{ user }` | Editar conta |
| GET | `/users/me/profile` | usuario logado | - | `{ profile }` | Carregar onboarding |
| PUT | `/users/me/profile` | usuario logado | `objective, seniority, targetRoles[], preferredLocations[], preferredWorkModel, salaryExpectation, mustHaveSkills[], niceToHaveSkills[]` | `{ profile }` | Salvar perfil |
| POST | `/users/me/resume` | usuario logado | `multipart/form-data` com `file` PDF | `{ resume }` | Upload curriculo |
| GET | `/users/me/resume` | usuario logado | - | `{ resume }` | Exibir curriculo ativo |
| DELETE | `/users/me/resume` | usuario logado | - | `{ deleted: true }` | Remover curriculo |

Notas de curriculo:

- `parseStatus` pode vir como `COMPLETED`, `FALLBACK` ou `FAILED`.
- Exibir `parseConfidence` e um aviso de confianca baixa quando necessario.

### 4.3 Pipeline (usuario)

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| POST | `/pipeline/run` | usuario logado | `{ force=false, daysRange=7, dateFrom?, dateTo?, forceRescore=false }` | `{ runId, lockTtlSeconds, dateFrom, dateTo, daysRange, forceRescore }` | Disparo manual |
| GET | `/pipeline/status` | usuario logado | - | `{ jobsCount, applicationsCount, isRunning, activeRunId, activeRunTtlSeconds, activeRunMetrics, lastRun }` | Barra de progresso e metricas |

Campos relevantes de metricas (`activeRunMetrics` / `lastRun.metrics`):

- `jobsProcessed`, `jobsFailed`, `jobsFinished`
- `aiCalls`, `aiCacheHits`, `aiCacheMisses`, `aiCacheHitRate`
- `aiSkipped`, `aiPrefilterRejected`, `aiPrefilterReasons`

### 4.4 Pipeline admin (operacional)

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| POST | `/pipeline/global/run` | ADMIN | `{ force=false, daysRange=14 }` | `{ runId, daysRange, lockTtlSeconds }` | Ingestao global |
| GET | `/pipeline/global/status` | ADMIN | - | `{ isRunning, activeRunId, activeRunTtlSeconds, lastRun }` | Monitor global |
| POST | `/pipeline/global/catalog-cleanup/run` | ADMIN | - | `{ runId, status, trigger, retentionDays, batchSize, deletedJobs, ... }` | Limpeza manual |
| GET | `/pipeline/global/catalog-cleanup/status` | ADMIN | - | `{ isRunning, activeRunId, activeRunTtlSeconds, lastRun }` | Monitor cleanup |

### 4.5 Vagas e ranking

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| GET | `/jobs` | usuario logado | `?limit=20&offset=0` | `{ jobs, pagination }` | Lista geral |
| POST | `/jobs` | usuario logado | `JobCreateRequest` | `{ job }` | Criacao manual |
| GET | `/jobs/{job_id}` | usuario logado | path `job_id` | `{ job }` | Detalhe simples |
| PUT | `/jobs/{job_id}` | usuario logado | `JobUpdateRequest` | `{ job }` | Edicao |
| DELETE | `/jobs/{job_id}` | usuario logado | path `job_id` | `{ jobId }` | Exclusao |
| GET | `/jobs/ranking/daily` | usuario logado | `?limit&offset&daysRange` ou `?dateFrom&dateTo` | `{ ranking, window, pagination }` | Ranking principal |
| GET | `/jobs/{job_id}/score` | usuario logado | path `job_id` | `{ jobScore }` | Explicabilidade |

Notas de ranking:

- Filtra por `effectiveDate = sourcePostedAt || firstSeenAt`.
- `window` sempre retorna `dateFrom`, `dateTo`, `daysRange` resolvidos.

### 4.6 Aplicacoes

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| GET | `/applications` | usuario logado | `?limit&offset&status?` | `{ applications, pagination }` | Funil |
| POST | `/applications` | usuario logado | `{ jobId, status='PENDING', channel='MANUAL', notes? }` | `{ application }` | Criar candidatura |
| GET | `/applications/{application_id}` | usuario logado | path | `{ application }` | Detalhe |
| PUT | `/applications/{application_id}` | usuario logado | `{ status?, notes? }` | `{ application }` | Atualizar etapa |
| DELETE | `/applications/{application_id}` | usuario logado | path | `{ applicationId }` | Remover |

### 4.7 Feedback

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| GET | `/feedback` | usuario logado | `?limit&offset&rating?` | `{ feedback, pagination }` | Historico |
| POST | `/feedback` | usuario logado | `{ jobId, rating(1..5), notes? }` | `{ feedback, scoreWeights }` | Captura de qualidade |
| GET | `/feedback/{feedback_id}` | usuario logado | path | `{ feedback }` | Detalhe |
| PUT | `/feedback/{feedback_id}` | usuario logado | `{ rating?, notes? }` | `{ feedback, scoreWeights }` | Ajuste feedback |
| DELETE | `/feedback/{feedback_id}` | usuario logado | path | `{ feedbackId }` | Remover |

### 4.8 Digest

| Metodo | Endpoint | Auth | Input | Retorno em `data` | Uso na UI |
|---|---|---|---|---|---|
| POST | `/digest/generate` | usuario logado | `{ digestDate? }` | `{ digest }` | Gerar resumo |
| GET | `/digest/daily` | usuario logado | `?digestDate?` | `{ digest }` | Ler resumo |

## 5) Recomendacoes de UX de alto impacto

- **Pipeline-first UX**: CTA principal "Atualizar ranking" com seletor de janela (7/14/30 ou custom).
- **Feedback visual imediato**: progresso com `jobsProcessed/jobsFinished` e chips de prefilter reasons.
- **Ranking explicavel**: sempre abrir score breakdown no detalhe para gerar confianca.
- **Acoes proximas da vaga**: aplicar, avaliar (1..5), salvar nota, tudo no mesmo contexto.
- **Admin separado**: area operacional isolada com confirmacoes para global run/cleanup.

## 6) Erros e resiliencia no frontend

- `400`: mostrar `detail` em toast/banner contextual.
- `401`: limpar sessao local e redirecionar para login.
- `403`: esconder recursos admin e mostrar aviso de permissao.
- `429`: exibir "aguarde alguns instantes" com retry exponencial.

## 8) Checklist de entrega frontend

- Login/cadastro/reset funcionando com cookie HttpOnly.
- Onboarding (perfil + curriculo) concluido antes do primeiro run.
- Polling de pipeline implementado com timeout e retry.
- Ranking com filtro por janela, paginacao e detalhe de score.
- Aplicacoes/feedback/digest conectados e com empty states.
- Painel admin com global run/status + cleanup run/status.
