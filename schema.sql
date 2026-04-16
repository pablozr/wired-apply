CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    fullname   VARCHAR(255) NOT NULL,
    email      VARCHAR(255) NOT NULL UNIQUE,
    password   VARCHAR(255) NOT NULL,
    role       VARCHAR(50)  NOT NULL DEFAULT 'BASIC',
    created_at TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_profiles (
    id                   SERIAL PRIMARY KEY,
    user_id              INTEGER      NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    objective            TEXT,
    seniority            VARCHAR(50),
    target_roles         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    preferred_locations  JSONB        NOT NULL DEFAULT '[]'::jsonb,
    preferred_work_model VARCHAR(30),
    salary_expectation   VARCHAR(100),
    must_have_skills     JSONB        NOT NULL DEFAULT '[]'::jsonb,
    nice_to_have_skills  JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at           TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_resumes (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_name        VARCHAR(255) NOT NULL,
    mime_type        VARCHAR(100) NOT NULL DEFAULT 'application/pdf',
    file_size        INTEGER      NOT NULL CHECK (file_size > 0),
    file_hash        VARCHAR(64)  NOT NULL,
    file_content     BYTEA        NOT NULL,
    extracted_text   TEXT,
    extracted_json   JSONB        NOT NULL DEFAULT '{}'::jsonb,
    parse_status     VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    parse_confidence NUMERIC(5,2),
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS user_resumes_user_created_idx
    ON user_resumes (user_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS user_resumes_user_active_idx
    ON user_resumes (user_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS user_resumes_file_hash_idx
    ON user_resumes (file_hash);

CREATE TABLE IF NOT EXISTS jobs (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title           VARCHAR(255) NOT NULL,
    company         VARCHAR(255) NOT NULL,
    location        VARCHAR(255),
    source          VARCHAR(50)  NOT NULL DEFAULT 'manual',
    source_url      TEXT,
    external_job_id VARCHAR(255),
    status          VARCHAR(30)  NOT NULL DEFAULT 'NEW',
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_user_source_external_job_idx
    ON jobs (user_id, source, external_job_id)
    WHERE external_job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_scores (
    id                  SERIAL PRIMARY KEY,
    user_id             INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id              INTEGER      NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    score               NUMERIC(5,2) NOT NULL DEFAULT 0,
    deterministic_score NUMERIC(5,2),
    ai_score            NUMERIC(5,2),
    ai_confidence       NUMERIC(5,2),
    final_score         NUMERIC(5,2),
    bucket              VARCHAR(1)   NOT NULL DEFAULT 'C',
    reason              TEXT,
    ai_reason           TEXT,
    created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, job_id)
);

CREATE INDEX IF NOT EXISTS job_scores_user_final_score_idx
    ON job_scores (user_id, final_score DESC NULLS LAST, updated_at DESC);

CREATE TABLE IF NOT EXISTS applications (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id     INTEGER     NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    status     VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    channel    VARCHAR(30) NOT NULL DEFAULT 'MANUAL',
    notes      TEXT,
    applied_at TIMESTAMP,
    created_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, job_id)
);

CREATE TABLE IF NOT EXISTS user_feedback (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id     INTEGER   NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    rating     SMALLINT  NOT NULL CHECK (rating BETWEEN 1 AND 5),
    notes      TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, job_id)
);

CREATE TABLE IF NOT EXISTS daily_digest (
    id                 SERIAL PRIMARY KEY,
    user_id            INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    digest_date        DATE      NOT NULL DEFAULT CURRENT_DATE,
    total_jobs         INTEGER   NOT NULL DEFAULT 0,
    total_applications INTEGER   NOT NULL DEFAULT 0,
    total_interviews   INTEGER   NOT NULL DEFAULT 0,
    payload            JSONB     NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, digest_date)
);

CREATE TABLE IF NOT EXISTS score_weights (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER      NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    role_weight      NUMERIC(5,2) NOT NULL DEFAULT 0.35,
    salary_weight    NUMERIC(5,2) NOT NULL DEFAULT 0.25,
    location_weight  NUMERIC(5,2) NOT NULL DEFAULT 0.20,
    seniority_weight NUMERIC(5,2) NOT NULL DEFAULT 0.20,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

ALTER TABLE job_scores
    ADD COLUMN IF NOT EXISTS deterministic_score NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS ai_score NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS ai_confidence NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS final_score NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS ai_reason TEXT;

UPDATE job_scores
SET
    deterministic_score = COALESCE(deterministic_score, score),
    final_score = COALESCE(final_score, score),
    score = COALESCE(score, final_score, deterministic_score, 0)
WHERE
    deterministic_score IS NULL
    OR final_score IS NULL
    OR score IS NULL;
