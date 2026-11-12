const { Worker } = require('bullmq');
const { make } = require('./redis');
const { QUEUE_NAME, DRIVERS, MAX_OFFERS, ACCEPT_RATE, WORKER_CONCURRENCY } = require('./config');
const { transition, getRide } = require('./store');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Fisher-Yates shuffle -> pick MAX_OFFERS DISTINCT drivers, so a ride is never
// offered to the same driver twice.
function pickDrivers(n) {
  const a = [...DRIVERS];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, n);
}

// Fake driver response: network-ish delay, then random accept/reject.
async function offerToDriver(rideId, driverId) {
  await sleep(50 + Math.random() * 250);
  return Math.random() < ACCEPT_RATE;
}

async function processRide({ rideId }) {
  const ride = await getRide(rideId);
  if (!ride) return;                              // unknown ride, nothing to do
  if (ride.status !== 'REQUESTED') return;        // already final -> safe to retry / redeliver

  for (const driverId of pickDrivers(MAX_OFFERS)) {
    const accepted = await offerToDriver(rideId, driverId);
    console.log(`[worker] ride ${rideId.slice(0, 8)} offered to ${driverId}: ${accepted ? 'ACCEPT' : 'reject'}`);
    if (accepted) {
      // atomic: only succeeds if the ride is still REQUESTED -> no double assignment
      await transition(rideId, 'REQUESTED', 'ASSIGNED', driverId);
      return;
    }
  }
  await transition(rideId, 'REQUESTED', 'NO_DRIVER_FOUND');
}

const worker = new Worker(QUEUE_NAME, (job) => processRide(job.data), {
  connection: make(),
  concurrency: WORKER_CONCURRENCY,
});

// If a job exhausts all retries (crash loop / bug), still reach a final status.
worker.on('failed', async (job, err) => {
  console.error(`[worker] job ${job?.id} failed: ${err.message}`);
  if (job && job.attemptsMade >= (job.opts.attempts || 1)) {
    await transition(job.data.rideId, 'REQUESTED', 'NO_DRIVER_FOUND').catch(() => {});
  }
});

console.log(`[worker] started (concurrency ${WORKER_CONCURRENCY})`);
