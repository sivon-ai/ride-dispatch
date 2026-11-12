// Task 3: book 100 rides at once, wait for them to settle, print the checks.
const { make } = require('../src/redis');
const { STREAM } = require('../src/config');

const API = process.env.API_URL || 'http://127.0.0.1:3000';
const N = Number(process.env.RIDES || 100);
const TIMEOUT_MS = 60000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FINAL = new Set(['ASSIGNED', 'NO_DRIVER_FOUND']);

async function main() {
  const redis = make();
  const base = {
    events: await redis.xlen(STREAM),
    billing: await redis.scard('seen:billing'),
    ops: await redis.scard('seen:ops'),
  };

  console.log(`Booking ${N} rides concurrently against ${API} ...`);
  const responses = await Promise.all(
    Array.from({ length: N }, (_, i) =>
      fetch(`${API}/rides`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ riderId: `rider-${i}`, pickup: 'Bandra', dropoff: 'Andheri' }),
      }).then((r) => r.json()).catch(() => null)),
  );
  const rideIds = responses.filter((r) => r && r.rideId).map((r) => r.rideId);

  // poll until every ride has a final status (or timeout)
  let rides = [];
  const start = Date.now();
  while (Date.now() - start < TIMEOUT_MS) {
    rides = await Promise.all(rideIds.map((id) => fetch(`${API}/rides/${id}`).then((r) => r.json())));
    if (rides.every((r) => FINAL.has(r.status))) break;
    await sleep(500);
  }

  // let the two event consumers catch up
  const expectedEvents = base.events + N * 2; // REQUESTED + one final event per ride
  const catchUp = Date.now();
  while (Date.now() - catchUp < 10000) {
    const [b, o] = [await redis.scard('seen:billing'), await redis.scard('seen:ops')];
    if (b - base.billing >= N * 2 && o - base.ops >= N * 2) break;
    await sleep(300);
  }

  const assigned = rides.filter((r) => r.status === 'ASSIGNED').length;
  const noDriver = rides.filter((r) => r.status === 'NO_DRIVER_FOUND').length;
  const doubleAssigned = rides.filter((r) => r.assignments.length > 1).length;
  const stuck = rides.filter((r) => !FINAL.has(r.status)).length;
  const eventsNow = (await redis.xlen(STREAM)) - base.events;
  const billingSeen = (await redis.scard('seen:billing')) - base.billing;
  const opsSeen = (await redis.scard('seen:ops')) - base.ops;

  const rows = [
    { Check: 'Total rides created', Expected: N, Actual: rideIds.length },
    { Check: 'ASSIGNED + NO_DRIVER_FOUND', Expected: N, Actual: assigned + noDriver },
    { Check: '  of which ASSIGNED', Expected: '~87', Actual: assigned },
    { Check: '  of which NO_DRIVER_FOUND', Expected: '~13', Actual: noDriver },
    { Check: 'Rides assigned to 2+ drivers', Expected: 0, Actual: doubleAssigned },
    { Check: 'Rides stuck with no final status', Expected: 0, Actual: stuck },
    { Check: 'Events published', Expected: N * 2, Actual: eventsNow },
    { Check: 'Events seen by billing', Expected: N * 2, Actual: billingSeen },
    { Check: 'Events seen by ops', Expected: N * 2, Actual: opsSeen },
  ];
  console.table(rows);

  const pass =
    rideIds.length === N && assigned + noDriver === N && doubleAssigned === 0 && stuck === 0 &&
    eventsNow === N * 2 && billingSeen === N * 2 && opsSeen === N * 2;
  console.log(pass ? 'RESULT: PASS' : 'RESULT: FAIL');
  await redis.quit();
  process.exit(pass ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
