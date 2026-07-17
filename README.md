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
- **Logging is fully decoupled from the hot path.** `POST /v1/check`
  never awaits a database write — it fires an async `XADD` to a durable
  Redis Stream (`request_logs`) and returns immediately. A background worker
  group reads this stream and flushes to Postgres with at-least-once 
  delivery semantics and `XAUTOCLAIM` crash recovery (`src/logging/queue.ts`).
  A separate rollup job (`src/logging/rollup.ts`) pre-aggregates into 
  `usage_daily` so dashboard trend queries never scan raw request logs.
- **Config:** per-client limits live in Postgres (`clients` table) but
  are cached in-process. Updates via the API trigger an immediate refresh
  across all instances via Redis Pub/Sub (`config:refresh`), so the check
  path never does a DB round-trip.

## Verified behavior (not just claimed — see "What's actually been tested" below)

- 8 simulated cluster nodes firing 300 concurrent requests at a
  50-capacity bucket admit **exactly 50**, never more — proven in
  `test/race.test.ts` against a real Redis instance.
- Killing Redis mid-traffic: checks keep succeeding via the local
  fallback (`source: "fallback"` in the response), `/healthz` reports
  `breaker: "OPEN"`, and traffic is never blocked. Reconnecting Redis
  self-heals the breaker back to `CLOSED` within one cooldown window.
- End-to-end: `POST /v1/check` → async log → Postgres → rollup →
  `GET /v1/usage/:clientId` returns correct aggregate counts and avg
  latency.

## Running it

```bash
docker compose up --build
```

This starts Redis, Postgres, **two** independent API replicas (`api1`,
`api2` — the "cluster"), and an nginx load balancer in front of them.

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
curl http://localhost:8080/v1/usage/client-a?range=24h -H 'Authorization: Bearer my-secret-key-123'
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

## Local development (without Docker)

Requires Node 22+, a local Redis, and a local Postgres.

```bash
npm install
export PGHOST=localhost PGUSER=ratelimiter PGPASSWORD=ratelimiter PGDATABASE=ratelimiter REDIS_HOST=localhost
npm run dev          # ts-node, runs migrations automatically on boot
```

## What's actually been tested

```bash
npm test
```

Runs 19 tests across four suites, all against **real** infrastructure
(no mocks for Redis):

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
| `POST` | `/v1/check` | `{ clientId, cost? }` → `{ allowed, remaining, retryAfterMs, source }` |
| `GET` | `/v1/clients` | list configured clients and their limits |
| `PUT` | `/v1/clients/:clientId` | upsert a client's `name`, `capacity`, `refillRatePerSec` |
| `GET` | `/v1/usage/:clientId?range=60m\|24h\|7d` | aggregated usage with dynamic time granularity |
| `GET` | `/healthz` | breaker state + log queue depth |
| `GET` | `/dashboard/` | static usage dashboard (Chart.js) |

## Known trade-offs / next steps

- **Horizontal Scaling Limits:** Redis is extremely fast, but a single
  Redis node limits total cluster throughput. Redis Cluster support would 
  be required to scale beyond ~100k requests/second.
