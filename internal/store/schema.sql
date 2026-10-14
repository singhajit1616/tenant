CREATE TABLE IF NOT EXISTS tenants (
    id          UUID PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,
    version     BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id          UUID PRIMARY KEY,
    type        TEXT NOT NULL,
    tenant_id   UUID NOT NULL REFERENCES tenants (id),
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS tasks_tenant_id_idx ON tasks (tenant_id);
CREATE INDEX IF NOT EXISTS tasks_created_at_idx ON tasks (created_at DESC);

CREATE TABLE IF NOT EXISTS outbox (
    id            UUID PRIMARY KEY,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    published_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_updates (
    update_id     TEXT PRIMARY KEY,
    task_id       UUID NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL
);
