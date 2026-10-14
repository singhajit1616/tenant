# Tenant Provisioning Control Plane

A Go REST API and asynchronous worker simulator. PostgreSQL is the source of
truth; RabbitMQ carries persisted tasks and worker progress.

```text
HTTP client --> control plane :8080 --> PostgreSQL
                    |                  tenants + tasks + outbox (one transaction)
                    |                  processed_updates (idempotency)
                    |
                outbox relay -- after commit --> RabbitMQ
                    ^                               |
                    |                               v
                progress consumer <------------ worker simulator
```

## Cold checkout

Requires Docker Engine/Desktop with Compose v2 and Linux containers. No local
Go, database, broker, or Make installation is required.

```bash
git clone <your-public-repository-url>
cd <repository-directory>
docker compose up --build -d
docker compose --profile test run --rm --build test
```

The first Compose command builds and boots the API, worker, PostgreSQL, and
RabbitMQ. After startup, `curl http://localhost:8080/readyz` returns
`{"status":"ready"}`. API: `http://localhost:8080`.

The second command is the full quality gate: formatting verification, `go vet`
static analysis, `govulncheck` security scanning, and the complete automated suite
with the race detector. Scanner failures are never suppressed. The scanner needs
internet access to the Go vulnerability database. Build and test images use Go
1.26.9; `go.mod` requires Go 1.26 and selects the patched 1.26.9 toolchain.

Tests use separate PostgreSQL and RabbitMQ services and a unique database schema per
test. They never truncate application data. Missing integration configuration
fails this gate rather than silently skipping tests. CI uses the same command.
CI also boots the actual app containers and runs `python3 scripts/smoke.py`, a
standard-library HTTP smoke test of the complete lifecycle and concurrent create
and PATCH races. You can run that script after boot when Python 3 is available.

```bash
docker compose logs -f controlplane worker
docker compose down                # stop; preserve application data and messages
docker compose down -v             # explicitly erase all data
```

PostgreSQL and RabbitMQ named volumes survive container replacement.

## API walkthrough

These Bash examples use `curl` and optional `jq` to capture IDs and versions.
Those tools are only needed for the examples. PowerShell users can use
`Invoke-RestMethod` and its `.id` / `.version` properties instead.

```bash
TENANT_ID=$(curl -fsS -X POST http://localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"acme-prod","name":"Acme"}' | jq -r .id)
curl -fsS http://localhost:8080/v1/tenants/$TENANT_ID
curl -fsS http://localhost:8080/v1/tenants/$TENANT_ID/tasks
```

The worker automatically drives deploy `accepted → in_progress → done` and
tenant `provisioning → active`. Poll until active before updating or deleting.

```bash
VERSION=$(curl -fsS http://localhost:8080/v1/tenants/$TENANT_ID | jq -r .version)
curl -fsS -X PATCH http://localhost:8080/v1/tenants/$TENANT_ID \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"Acme Ltd\",\"version\":$VERSION}"

# Wait until active again, then destroy:
curl -fsS -X DELETE http://localhost:8080/v1/tenants/$TENANT_ID
curl -fsS http://localhost:8080/v1/tenants/$TENANT_ID/tasks
```

Versions increment on API changes and each applied worker progress mutation.
The normal lifecycle reaches version **3** after deploy, **6** after update, and
**9** after destroy. Always fetch the version: skipped/stale updates do not
increment it. Timestamps use UTC ISO 8601 with milliseconds and a `Z` suffix.

| Method | Resource | Success / behavior |
| --- | --- | --- |
| POST | `/v1/tenants` | 201; tenant body, provisioning; creates deploy task |
| GET | `/v1/tenants` | 200; paginated tenants, including destroyed |
| GET | `/v1/tenants/{id}` | 200; tenant body |
| PATCH | `/v1/tenants/{id}` | 200; tenant body, updating; positive version required, name optional; only active |
| DELETE | `/v1/tenants/{id}` | 202; tenant body, destroying; only active or failed |
| GET | `/v1/tenants/{id}/tasks` | 200; paginated tasks; missing tenant is 404 |
| GET | `/v1/tasks` | 200; optional tenant_id UUID and task status filters |
| GET | `/v1/tasks/{id}` | 200; task body |
| GET | `/healthz` | 200; HTTP process alive |
| GET | `/readyz` | 200 if database and broker connected; otherwise 503 |

Lists return `{"items":[],"limit":20,"offset":0,"total":0}`. Default limit is
20, capped at 100; offset defaults to zero. Invalid pagination values use the
defaults. IDs must be UUIDs. Unknown body fields, trailing JSON, empty names, and
invalid slugs are rejected. Bodies are capped at 1 MiB. Slug is immutable and
unique forever: `^[a-z][-a-z0-9]{1,26}[a-z0-9]$` (3–28 characters).

All API errors, including routing errors and recovered panics, return JSON:

```json
{"error":{"code":"tenant_not_found","message":"tenant not found"}}
```

| Code | HTTP |
| --- | --- |
| invalid_request | 400 |
| tenant_not_found, task_not_found, route_not_found | 404 |
| method_not_allowed | 405 |
| tenant_already_exists, tenant_update_not_allowed, tenant_version_conflict | 409 |
| internal_error | 500 |
| not_ready | 503 |

PATCH checks existence, version, then state under a row lock. Concurrent callers
using the same version yield one success and version conflicts for losers.
DELETE uses the same row lock and state guards. A version-only PATCH starts an
update operation. Tasks are read-only over HTTP.

## Reproduce negative scenarios

**Failed provisioning:** restart the existing worker with guaranteed failure,
create a tenant, then poll its tenant/tasks endpoints until both reach failed:

```bash
FAIL_RATE=1 MIN_DELAY_MS=100 MAX_DELAY_MS=200 docker compose up -d --force-recreate worker
curl -sS -X POST http://localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"will-fail","name":"Failure demo"}'
FAIL_RATE=0 docker compose up -d --force-recreate worker
```

DELETE is allowed from failed and creates a destroy task. Wait for failure before
restoring the successful worker. In PowerShell set `$env:FAIL_RATE="1"` before
Compose, then restore `$env:FAIL_RATE="0"`. Delay variables are MIN_DELAY_MS and
MAX_DELAY_MS. Worker CLI flags `--fail-rate`, `--min-delay-ms`, `--max-delay-ms`
override the environment.

**Duplicate slug:** POST the same slug twice. The second is 409 /
tenant_already_exists, including after destruction. Concurrent creates and
PATCHes are tested with multiple goroutines.

**Duplicate/stale progress:** use an existing completed task ID:

```bash
TASK_ID=<task-uuid>
docker compose --profile tools run --rm --build inject \
  --task-id "$TASK_ID" --status in_progress --update-id stale-1
docker compose --profile tools run --rm inject \
  --task-id "$TASK_ID" --status done --update-id dup-1
docker compose --profile tools run --rm inject \
  --task-id "$TASK_ID" --status done --update-id dup-1
```

State, versions, and timestamps remain unchanged. For terminal-first delivery,
stop the worker, create a tenant, obtain its accepted task ID, inject done, then
in_progress. The tenant becomes active and cannot regress. Restart the worker.
RabbitMQ does not deduplicate by MessageId. The integration suite publishes
duplicate IDs to test database idempotency and sends malformed messages before
valid updates.

**Broker outage:** stop RabbitMQ, create a tenant, then start RabbitMQ:

```bash
docker compose stop rabbitmq
curl -sS -X POST http://localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"broker-outage","name":"Recovery demo"}'
docker compose start rabbitmq
```

The API commits the tenant/task/outbox while delivery waits. Readiness is 503
during disconnection. Relay and worker recover automatically when RabbitMQ returns.
For persistence, `docker compose down`, `docker compose up -d`, then GET the same
tenant. Do not add `-v` when testing persistence.

## Messaging and trade-offs

Outbound `tasks.outbound` carries the task as its event:

```json
{"id":"<task-uuid>","type":"deploy","tenant_id":"<tenant-uuid>","status":"accepted"}
```

Inbound `tasks.progress` carries a stable update key:

```json
{"update_id":"<task-uuid>:in_progress","task_id":"<task-uuid>","status":"in_progress"}
```

- Tenant, task, and one outbox record are committed together. The relay only sees
  committed records; rollback never emits a task.
- Delivery is **at least once**. A crash after publishing but before recording
  published_at can repeat delivery. Task IDs and update IDs are AMQP MessageId
  metadata; RabbitMQ does not deduplicate them. Database idempotency is
  authoritative. No exactly-once transport claim is made.
- The inbound transaction claims the unique update_id before applying changes.
  Concurrent duplicates cannot both mutate state. Duplicate/stale updates preserve
  versions and timestamps. First terminal outcome wins. Unknown tasks are recorded
  and ignored because legitimate tasks are committed before publication.
- Terminal-first updates apply directly so reordered progress cannot strand a
  task. Normal worker execution still reports both steps. Terminal tasks never
  regress. Tenant failure is reachable from deploy, update, and destroy.
- Worker keys are stable per task/stage (`:in_progress`, `:terminal`). Failure
  sampling is deterministic per task with unchanged configuration, preserving
  simulated outcomes across replay. Changed configuration affects unfinished work;
  already-terminal tasks retain their outcome.
- Relay and worker publication retry with capped exponential delays. Consumer
  handlers retry transient failures while holding a delivery unacknowledged,
  with exponential delays capped at 30 seconds. Invalid progress is logged and
  acknowledged rather than retried forever. Losing a channel or process causes
  RabbitMQ to requeue unacknowledged deliveries.
- PostgreSQL uniqueness arbitrates concurrent slug creation. Tenant row locks
  and state guards allow one open task per tenant without another application lock.
- RabbitMQ uses durable classic queues, persistent messages, manual acknowledgements,
  and publisher confirmations. Publishing uses the default exchange with mandatory
  routing, so an unroutable message is a failure. An outbox row is marked published
  only after a positive confirmation. Consumer prefetch is one; channels are
  recreated on a lazily reconnected connection after failures. Broker storage uses
  a named volume and a stable hostname. Compose waits for broker health at startup.
  Consumer restart retries are handled inside the broker package so the control
  plane and worker do not maintain separate reconnection loops.

Processing is sequential per worker, migration is an idempotent embedded schema,
and pagination uses offsets. History has no retention cleanup; production would
need retention policies and monitoring. Authentication and real provisioning
are outside this simulator's scope.

## Tests and layout

The full quality gate covers domain transitions, database lifecycle/failure
outcomes and versions, slug uniqueness, concurrent PATCH, concurrent idempotency
keys, rollback, outbox retries, persistence across reopened connections, API error
contracts, and HTTP-to-worker-to-consumer integration. A TCP proxy interrupts
real broker connections to verify recovery without granting Docker socket access.

For development, `go test ./...` runs unit tests and explicitly skips tests that
need missing services. This is **not** the submission gate; use Compose to require
all integration tests.

```text
cmd/controlplane       HTTP server, relay, progress consumer
cmd/worker             configurable simulator
cmd/inject             direct progress publisher
internal/domain        models and state rules
internal/store         PostgreSQL transactions and schema
internal/api           REST and JSON errors
internal/broker        RabbitMQ, publisher confirmations, retries
internal/outbox        after-commit publisher
internal/consumer      progress validation/application
internal/integration   lifecycle and broker interruption tests
internal/testutil      isolated database test schemas
scripts/quality.sh     full quality gate
```

Compose passwords are public local-development defaults, not external credentials.
Never commit real credentials or `.env` files.

RabbitMQ management UI: http://localhost:15672 (local development login: tenant / tenant).
AMQP listens on port 5672. All processes use `RABBITMQ_URL`; the inject CLI also
accepts `--rabbitmq-url`. Queues are `tasks.outbound` and `tasks.progress`.
For consumption exceeding RabbitMQ's acknowledgement timeout (normally 30 minutes),
configure that timeout appropriately; unacknowledged deliveries are otherwise requeued.
RabbitMQ semantics: [publisher confirmations](https://www.rabbitmq.com/docs/confirms)
and [reliability](https://www.rabbitmq.com/docs/reliability).
