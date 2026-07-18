# Global Rate Limiter as a Service

A high-availability rate limiter for outbound calls to quota-constrained
third-party APIs (banking, logistics, AI providers). Any number of
service instances can query it; correctness does not depend on which
instance answers.

See `diagrams/architecture.png` for the full component diagram.

## Design summary

- **Algorithm:** token bucket, per client, stored in Redis as a hash
  (`tokens`, `ts`). Checked and consumed **atomically** in a single Lua
  script (`src/ratelimiter/tokenbucket.lua`) run via `EVALSHA`, so
  concurrent callers from different instances can never race each other
  — Redis executes Lua single-threaded. Redis's own `TIME` command is
  used inside the script (not a timestamp passed in by the caller), so
  clock drift between instances can't cause over/under-admission.
- **Fail-safe:** a circuit breaker (`src/ratelimiter/circuitBreaker.ts`)
  watches Redis health. On repeated failure/timeout it opens and every
  instance falls back to its own **local in-memory token bucket**
  (`src/ratelimiter/fallback.ts`) for the duration of the outage. This
  is a deliberate fail-*open* choice: a client can briefly burst above
  its limit across N unaware instances during a Redis outage, but
  traffic is never blocked outright — see the brief's requirement that
  "the system must not block all traffic." The breaker self-heals via a
  half-open probe once Redis is reachable again.
- **Logging is fully decoupled from the hot path, and durable.**
  `POST /v1/check` never awaits a database write — it fires an `XADD`
  onto a Redis Stream (`stream:request_log`, `src/logging/streamProducer.ts`)
  and returns immediately. A separate `worker` process
  (`src/worker.ts` / `src/logging/streamConsumer.ts`) reads the stream
  via a **consumer group**, batch-inserts into Postgres, and only
  `XACK`s after the insert succeeds. If a worker dies between reading
  and acking, that entry sits in the group's pending list and gets
  reclaimed by another worker via `XAUTOCLAIM` — nothing is silently
  dropped on a process crash, which an in-memory queue can't promise.
  A separate rollup job (`src/logging/rollup.ts`) pre-aggregates into
  `usage_daily` so dashboard trend queries never scan raw request logs.
- **Config:** per-client limits live in Postgres (`clients` table) but
  are cached in-process and refreshed on a 5s timer, so the check path
  never does a DB round-trip either.

## Verified behavior (not just claimed — see "What's actually been tested" below)

- 8 simulated cluster nodes firing 300 concurrent requests at a
  50-capacity bucket admit **exactly 50**, never more — proven in
  `test/race.test.ts` against a real Redis instance.
- Killing Redis mid-traffic: checks keep succeeding via the local
  fallback (`source: "fallback"` in the response), `/healthz` reports
  `breaker: "OPEN"`, and traffic is never blocked. Reconnecting Redis
  self-heals the breaker back to `CLOSED` within one cooldown window.
- End-to-end: `POST /v1/check` → Redis Stream → worker → Postgres →
  rollup → `GET /v1/usage/:clientId` returns correct aggregate counts
  and avg latency.
- **Logging survives a worker crash.** Killed a running worker process
  mid-traffic, kept sending checks (entries queue up in Redis, group
  `lag` grows, nothing errors), then started a fresh worker and watched
  `lag` drop back to 0 with every entry landing in Postgres — see
  `test/streamLogging.test.ts` for the automated version, which
  specifically simulates a crash (read-but-never-acked) and asserts
  `XAUTOCLAIM` recovers it.

## Running it

```bash
docker compose up --build
```

This starts Redis, Postgres, **two** independent API replicas (`api1`,
`api2` — the "cluster"), an nginx load balancer in front of them, and
**two** log-worker replicas (`worker1`, `worker2`) consuming
`stream:request_log` as a consumer group — the same "either instance
can take over" property applies to logging, not just checks.

- API (via load balancer): `http://localhost:8080`
- Dashboard: `http://localhost:8080/dashboard/`
- Direct access to a single replica (bypassing the LB, useful for
  demonstrating "either instance can answer"): `http://localhost:3000`
  / a second replica isn't published directly by default — go through
  8080 to see requests round-robin across `api1`/`api2`.

Two demo clients are seeded by the migration: `client-a` (100 req/min,
burst 100) and `client-b` (5000 req/min, burst 5000).

### Try it

```bash
# allowed
curl -X POST http://localhost:8080/v1/check \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret-key-123' \
  -d '{"clientId":"client-a"}'

# drain the burst, then see a 429
for i in $(seq 1 101); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8080/v1/check \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer my-secret-key-123' \
    -d '{"clientId":"client-a"}'
done

# usage dashboard data
curl http://localhost:8080/v1/usage/client-a?days=10 -H 'Authorization: Bearer my-secret-key-123'
```

### Verifying the fail-safe edge case yourself

```bash
docker compose stop redis
curl -X POST http://localhost:8080/v1/check \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret-key-123' \
  -d '{"clientId":"client-a"}'
# -> still 200/429 as appropriate, with "source": "fallback"
curl http://localhost:8080/healthz   # -> "breaker": "OPEN"

docker compose start redis
# after ~2s cooldown, breaker closes again automatically
```

### Verifying log durability yourself

```bash
docker compose stop worker1 worker2
for i in $(seq 1 20); do
  curl -s -o /dev/null -X POST http://localhost:8080/v1/check \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer my-secret-key-123' \
    -d '{"clientId":"client-a"}'
done
docker exec -it $(docker compose ps -q redis) redis-cli xinfo groups stream:request_log
# -> "lag" shows the 20 unconsumed entries, safely queued, not lost

docker compose start worker1
sleep 3
docker exec -it $(docker compose ps -q redis) redis-cli xinfo groups stream:request_log
# -> lag back to 0; check request_log in Postgres, all 20 rows present
```

## Local development (without Docker)

Requires Node 22+, a local Redis, and a local Postgres.

```bash
npm install
export PGHOST=localhost PGUSER=ratelimiter PGPASSWORD=ratelimiter PGDATABASE=ratelimiter REDIS_HOST=localhost
npm run dev          # ts-node, runs migrations automatically on boot

# in a second terminal -- without this, checks still work correctly,
# but approved/denied events just accumulate in the Redis stream
# instead of reaching Postgres/the dashboard
npm run worker
```

## What's actually been tested

```bash
npm test
```

Runs 27 tests across six suites, all against **real** infrastructure
(no mocks for Redis or Postgres):

- `test/fallback.test.ts` — local bucket algorithm: burst, refill,
  capacity cap, per-client isolation, idle sweep.
- `test/circuitBreaker.test.ts` — CLOSED → OPEN → HALF_OPEN → CLOSED
  state machine, including the single-in-flight-probe guard.
- `test/limiter.redis.test.ts` — the Redis-backed limiter against a
  live Redis: burst/deny, refill timing, per-client isolation,
  fractional cost, and a latency assertion (p50 < 5ms, p99 < 15ms for
  the check itself).
- `test/race.test.ts` — **the concurrency guarantee**: 8 simulated
  cluster nodes racing 300 concurrent requests against one 50-capacity
  bucket admit exactly 50.
- `test/streamLogging.test.ts` — **the durability guarantee**: entries
  published via `XADD` are correctly consumed and persisted; a
  simulated crash (read via `XREADGROUP`, never acked) is recovered by
  `XAUTOCLAIM` and still lands in Postgres; a failed Postgres insert
  leaves entries pending/retryable instead of silently dropping them.
- `test/clientTiers.test.ts` — tier inheritance, per-client override
  precedence, and that bumping a tier's numbers moves every client
  inheriting from it without touching their rows individually.

Load/throughput test (needs a running server, see below):

```bash
npm run build && npm start &
npm run loadtest
```

This reports full HTTP round-trip p50/p99 for a single unscaled
instance and asserts the service stays available (zero connection
errors/timeouts) under sustained concurrent load. It deliberately does
**not** gate on HTTP p99, because that number mixes in Express/Node
single-process overhead with the limiter's own performance — the
tight, meaningful latency bound on the check itself is asserted
separately in `limiter.redis.test.ts`. A single Node process saturating
its event loop under thousands of req/sec (and tail latency rising as
a result) is expected, and is exactly why the brief specifies a cluster
of instances behind a load balancer rather than a single process.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/check` | `{ clientId, cost? }` → `{ allowed, remaining, retryAfterMs, source }`, plus `RateLimit-Limit`/`RateLimit-Remaining` headers on every response and `Retry-After` on a 429 |
| `GET` | `/v1/clients` | list configured clients, their resolved limits, and `tierId` if inherited |
| `PUT` | `/v1/clients/:clientId` | upsert a client: either `{ name, tierId }` to inherit a tier's limits, or `{ name, capacity, refillRatePerSec }` for a bespoke override (both together is valid too — the explicit numbers win) |
| `GET` | `/v1/tiers` | list available tiers (`free`, `premium` by default) and their limits |
| `GET` | `/v1/usage/:clientId?days=10\|15\|30` | aggregated usage for the dashboard |
| `GET` | `/healthz` | breaker state + log stream publish-failure count |
| `GET` | `/dashboard/` | static usage dashboard (Chart.js) |

### Tiers

Inspired directly by the classic "free users get 100 req/hour, premium
get 1000 req/hour" example: rather than every client needing its own
hand-typed `capacity`/`refillRatePerSec`, a client can inherit from a
named tier instead, and any client can still override with bespoke
numbers regardless of tier. The resolution is a single `COALESCE` in
`ClientConfigStore.refresh()` — the `RateLimiter` and Lua script never
know or care whether a limit came from a tier or an override, so this
added zero complexity to the actual rate-limiting logic.

```bash
curl http://localhost:8080/v1/tiers -H 'Authorization: Bearer my-secret-key-123'
# [{"tierId":"free","capacity":100,...}, {"tierId":"premium","capacity":1000,...}]

# new client, inherits the premium tier's limits entirely
curl -X PUT http://localhost:8080/v1/clients/acme-corp \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret-key-123' \
  -d '{"name":"Acme Corp","tierId":"premium"}'

# bump every premium client's limit in one place
docker exec -it $(docker compose ps -q postgres) psql -U ratelimiter -d ratelimiter \
  -c "UPDATE tiers SET capacity = 2000, refill_rate_per_sec = 2000.0/3600 WHERE tier_id = 'premium';"
```

## Known trade-offs / next steps

- Client config refresh is poll-based (5s). Fine at this scale; a
  Postgres `LISTEN/NOTIFY` or Redis pub/sub push would tighten
  propagation delay if needed.
- No mTLS/auth between services in this demo compose file — would add
  an internal auth token or mesh in a real deployment.
- Tier *assignment* per client is fully dynamic (`PUT /v1/clients`),
  but tier *definitions* themselves (the numbers behind `free`/
  `premium`) currently only change via direct SQL or a migration —
  there's no `PUT /v1/tiers/:tierId` endpoint yet. Small, deliberately
  deferred addition.
- The Redis Stream is capped with an approximate `MAXLEN ~ 200000` on
  write, so a worker outage lasting long enough to exceed that would
  start losing the oldest unconsumed entries. Fine for this scale and
  timeline; a hard durability SLA would want either a much larger cap
  with disk-backed Redis persistence (AOF) or a dedicated broker.
