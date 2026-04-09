# FastAPI Backend Template

Template opinionado para APIs REST com **autenticação**, **gestão de utilizadores**, **PostgreSQL (asyncpg)**, **Redis**, **RabbitMQ** e **reset de password por e-mail**. O código evita ORMs, usa SQL parametrizado e separa camadas de forma explícita.

---

## Conteúdo

1. [Stack](#stack)
2. [Arquitetura em camadas](#arquitetura-em-camadas)
3. [Estrutura do repositório](#estrutura-do-repositório)
4. [Pré-requisitos](#pré-requisitos)
5. [Configuração inicial](#configuração-inicial)
6. [Executar a aplicação](#executar-a-aplicação)
7. [Rotas principais](#rotas-principais)
8. [Segurança e convenções](#segurança-e-convenções)
9. [Rate limiting](#rate-limiting)
10. [Worker de e-mail](#worker-de-e-mail)
11. [Estender o template](#estender-o-template)

---

## Stack

| Camada        | Tecnologia |
|---------------|------------|
| Linguagem     | Python 3.12+ |
| Framework     | FastAPI |
| Base de dados | PostgreSQL (`asyncpg`) |
| Cache         | Redis (`redis.asyncio`) |
| Mensagens     | RabbitMQ (`aio-pika`) |
| Configuração  | Pydantic Settings (`.env`) |
| Tokens        | PyJWT |
| Passwords     | bcrypt |
| Google login  | `google-auth` (validação do ID token) |

Não utiliza SQLAlchemy nem outro ORM: todo o acesso à base é SQL bruto com placeholders `$1`, `$2`, …

---

## Arquitetura em camadas

| Camada | Responsabilidade |
|--------|------------------|
| **`routes/`** | HTTP: parâmetros, dependências (`Depends`), cookies, `JSONResponse`. Rotas finas. |
| **`services/`** | Lógica de negócio e chamadas a BD, Redis e filas. Retorno padronizado `status`, `message`, `data`. |
| **`schemas/`** | Modelos Pydantic (pedidos), `TypedDict` onde fizer sentido e **mapeadores** DB → API (ex.: `user_from_row`). |
| **`core/`** | Configuração, singletons (PostgreSQL, Redis, RabbitMQ), segurança (JWT, hashing, rate limit), logging. |
| **`functions/utils/`** | Helpers transversais (`default_response`, `generate_temp_code`, etc.). |
| **`workers/`** | Processos à parte que consomem filas (ex.: envio SMTP). |
| **`templates/`** | Strings HTML para e-mail com placeholders. |

Constantes de aplicação (nomes de cookies, TTLs de rate limit, nome da fila de e-mail) ficam em **`core/config/config.py`**, junto de `settings`, não espalhadas pelos serviços.

---

## Estrutura do repositório

```text
├── core/
│   ├── config/config.py       # Settings (.env) + constantes da app
│   ├── logger/logger.py
│   ├── postgresql/postgresql.py
│   ├── redis/redis.py
│   ├── rabbitmq/rabbitmq.py
│   └── security/
│       ├── hashing.py         # bcrypt
│       ├── jwt_payloads.py    # dicts de claims JWT
│       ├── rate_limit.py      # Redis + dependencies pré-montadas
│       └── security.py        # JWT, Google, validate_token, RBAC
├── routes/
│   ├── auth/router.py
│   └── users/router.py
├── services/
│   ├── auth/auth_service.py
│   ├── user/user_service.py
│   ├── cache/cache_service.py
│   └── messaging/messaging_service.py
├── schemas/
│   ├── auth.py
│   └── user.py
├── functions/utils/utils.py
├── workers/smtp/email_worker.py
├── templates/email.py
├── main.py
├── schema.sql
├── requirements.txt
├── .env.example
└── README.md
```

Cada pasta de pacote Python inclui `__init__.py` onde aplicável.

---

## Pré-requisitos

- Python **3.12** ou superior (recomendado para wheels do `asyncpg` em Windows).
- Instâncias em execução de **PostgreSQL**, **Redis** e **RabbitMQ**.
- Conta SMTP (para fluxo de reset de password) e, se usar login Google, **Google Client ID** configurado no projeto.

---

## Configuração inicial

1. **Clonar / copiar** o template e entrar na pasta do projeto.

2. **Ambiente virtual** (exemplo):

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Variáveis de ambiente**: copiar `.env.example` para `.env` e preencher valores reais (ver secção seguinte).

4. **Base de dados**: criar a base e aplicar o script inicial:

   ```bash
   psql -U postgres -d your_db -f schema.sql
   ```

   (Ajustar utilizador, base e ferramenta conforme o seu ambiente.)

---

## Variáveis de ambiente (resumo)

| Grupo | Exemplos de chaves |
|--------|---------------------|
| Aplicação | `ENVIRONMENT`, `API_PORT` |
| PostgreSQL | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` |
| RabbitMQ | `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` |
| Redis | `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` |
| JWT | `SECRET_KEY`, `ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES` |
| SMTP | `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `EMAIL_FROM` |
| Google | `GOOGLE_CLIENT_ID` |

Lista completa e valores de exemplo: **`.env.example`**.

---

## Executar a aplicação

Na raiz do projeto (com `.env` carregado e serviços externos acessíveis):

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

- Documentação interativa: **`http://localhost:8000/docs`**
- OpenAPI JSON: **`http://localhost:8000/openapi.json`**

Em desenvolvimento, garanta que o diretório atual é a raiz do projeto para os imports `core.*`, `routes.*`, etc. funcionarem.

---

## Rotas principais

| Método | Caminho | Descrição |
|--------|---------|-----------|
| `POST` | `/auth/login` | Login com e-mail e password; cookie HttpOnly `auth`. |
| `POST` | `/auth/google-login` | Login com Google ID token. |
| `POST` | `/auth/logout` | Remove cookies de sessão e reset. |
| `POST` | `/auth/forget-password` | Inicia reset: código em Redis + e-mail na fila; cookie `auth_reset`. |
| `POST` | `/auth/validate-code` | Valida código de 6 dígitos; atualiza cookie `auth_reset`. |
| `POST` | `/auth/update-password` | Novo password após validação do código. |
| `GET`  | `/users/me` | Perfil do utilizador autenticado. |
| `PUT`  | `/users/me` | Atualiza perfil (campos permitidos). |
| `POST` | `/users/` | Registo público de utilizador. |

Prefixos: **`/auth`** e **`/users`** estão definidos em `main.py`.

---

## Segurança e convenções

- **Cookies**: `auth` (sessão) e `auth_reset` (fluxo de password); atributos `HttpOnly`, `Secure`, `SameSite=lax` conforme definido nas rotas.
- **JWT**: claims incluem `userId`, `email`, `fullname`, `role`, `type` (`auth` ou `reset`) e, no reset, `canUpdate`.
- **Identificador do utilizador**: na base de dados a coluna é `id`; em dicionários, JWT e respostas JSON públicas usa-se **`userId`** (ver `schemas.user.user_from_row`).
- **Passwords**: sempre bcrypt; verificação e hash em **`core/security/hashing.py`**.
- **Validação de token**: **`verify_token`** / **`validate_token`** em `core/security/security.py`, com recarregamento do utilizador em BD onde aplicável.
- **Rate limiting**: ver secção seguinte.

---

## Rate limiting

Rotas públicas sensíveis usam dependências Redis definidas em **`core/security/rate_limit.py`** (por exemplo `LOGIN_RATE_LIMIT_DEPS`, `FORGET_PASSWORD_RATE_LIMIT_DEPS`). Limites e janelas são ajustáveis via constantes `RATE_LIMIT_*` em **`core/config/config.py`**.

O endereço IP considera o cabeçalho **`X-Forwarded-For`** (primeiro IP), adequado a reverse proxy ou load balancer.

---

## Worker de e-mail

O envio SMTP não bloqueia a API: mensagens são publicadas na fila configurada (`EMAIL_QUEUE` em `core/config/config.py`). Para consumir e enviar e-mail, execute o worker a partir da **raiz** do projeto:

```bash
python -m workers.smtp.email_worker
```

Requer o mesmo `.env`, RabbitMQ ativo e credenciais SMTP válidas.

---

## Estender o template

Ao adicionar um novo domínio (ex.: produtos, encomendas):

1. `routes/<domínio>/router.py` e registo em `main.py`.
2. `services/<domínio>/<domínio>_service.py`.
3. `schemas/<domínio>.py` (sem modelos grandes inline nas rotas).
4. Migração SQL com `user_id` e `updated_at` onde fizer sentido para dados multi-inquilino.
5. Consultas com placeholders `$n`; `UPDATE` com `updated_at = NOW()`; dados pertencentes ao utilizador com condição explícita (ex.: `AND user_id = $x`).
6. Respostas padronizadas; em falhas, `"data": {}` obrigatório.

Regras detalhadas e checklist estão na skill do projeto (pasta **`.cursor/skills/`**), se a utilizar no Cursor.

---

## Licença

Este projeto está licenciado sob a **MIT License**: podes usar, copiar, modificar e distribuir o código livremente, desde que mantenhas o aviso de copyright e o texto da licença. Consulta o ficheiro [`LICENSE`](LICENSE) na raiz do repositório.

Se publicares um derivado, considera acrescentar o teu nome ou o da tua organização na linha de copyright do `LICENSE` (mantendo o aviso original se reutilizares partes substanciais deste template).
