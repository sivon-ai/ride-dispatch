# Ride Dispatch (intern backend task)

Simplified Ola/Uber-style assignment: **API -> queue -> worker -> event stream -> two independent consumers.**

## Architecture

```
POST /rides ──► API ──► Redis (ride=REQUESTED) ──► BullMQ queue "ride-offers"
                 │                                        │
                 │ returns rideId (202)                   ▼
                 │                                     Worker (offers to up to 3 distinct
                 │                                     fake drivers, ~50% accept each)
                 ▼                                        │
        Redis Stream "ride-events" ◄── every status change (REQUESTED / ASSIGNED / NO_DRIVER_FOUND)
             │                  │
   group "billing"        group "ops"      <- separate consumer groups = BOTH see EVERY event
             ▼                  ▼
      billing.js            ops.js
```

Stack: Node.js 20, Express, BullMQ (queue), Redis (state + queue + stream).

## Run it

```bash
docker compose up --build -d          # redis, api, worker, billing, ops
docker compose run --rm loadtest      # Task 3: 100 rides + checks table
docker compose logs billing ops       # see Task 2 output
```

Without Docker (needs Redis on localhost:6379):

```bash
npm install
npm run api &  npm run worker &  npm run billing &  npm run ops &
npm run loadtest
```

Try one ride manually:

```bash
curl -X POST localhost:3000/rides -H 'content-type: application/json' \
  -d '{"riderId":"r1","pickup":"Bandra","dropoff":"Andheri"}'
curl localhost:3000/rides/<rideId>
```

## How each requirement is met

**Task 1 - queue + worker.** `POST /rides` stores the ride as `REQUESTED`, enqueues a job and
returns `202 {rideId}` immediately. `src/worker.js` (a separate process) shuffles the 10 drivers, takes
3 *distinct* ones, and offers the ride in turn (random 50-300 ms delay, 50% accept). Accept -> `ASSIGNED`;
3 rejections -> `NO_DRIVER_FOUND`.

**Task 2 - fan-out events.** Redis Stream `ride-events`. Billing and ops each use their **own consumer
group**, so each group gets every event. (Two consumers in the *same* group would split the events.)
Groups are created from ID `0`, so a consumer started late still reads the full history. Events are
ack'ed only after handling, and un-acked entries are re-read on restart (at-least-once).

**Task 3 - load test.** `scripts/loadtest.js` fires 100 concurrent bookings, polls until all are final,
waits for both consumers to catch up, then prints the table and exits non-zero on failure.

## Correctness guarantees

| Risk | Mitigation |
|---|---|
| Ride assigned to 2 drivers | Status change is a Lua script: `if status == REQUESTED then set ASSIGNED`. Redis runs scripts atomically, so only one transition can ever win. Each assignment is also logged in `ride:<id>:assignments`, which the load test inspects. |
| Same driver offered twice | 3 distinct drivers are drawn up front by shuffle. |
| Ride stuck with no final status | Job has `attempts: 3` with backoff; a crashed worker's job is re-run (BullMQ stall detection). The worker skips rides that are already final, so retries are safe. If all attempts fail, the `failed` handler marks `NO_DRIVER_FOUND`. If enqueueing fails, the API marks the ride `NO_DRIVER_FOUND` and returns 503. |
| Status changed but event lost | The state change and `XADD` happen **in the same Lua script**, so they cannot diverge (no dual-write problem). |
| Event processed twice after crash | Consumers record entry IDs in a set; the "events seen" count is exact even with redelivery. |

## Trade-offs / what I'd do next

- Drivers never become "busy" after accepting (the task says a hardcoded fake list is enough).
- Redis is both queue and stream for simplicity; production would likely use Kafka/SQS+SNS and Postgres.
- Rides are never expired from Redis; real systems would add TTLs/archival.
- A retry after a worker crash restarts the offer sequence, so a driver could be offered twice across
  attempts (never assigned twice). Persisting offered drivers per ride would fix that.
- No auth, rate-limiting or input sanitising beyond required fields.

## Sample load-test output

```
Total rides created                100   100
ASSIGNED + NO_DRIVER_FOUND         100   100
Rides assigned to 2+ drivers         0     0
Rides stuck with no final status     0     0
Events published                   200   200
Events seen by billing             200   200
Events seen by ops                 200   200
RESULT: PASS
```
(With 3 tries at 50% each, expect ~87 assigned / ~13 NO_DRIVER_FOUND.)
