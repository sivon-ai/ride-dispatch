const express = require('express');
const { randomUUID } = require('crypto');
const { PORT } = require('./config');
const { createRide, transition, getRide } = require('./store');
const { queue } = require('./queue');

const app = express();
app.use(express.json());

// Task 1: book a ride. Returns immediately; the worker does the assignment.
app.post('/rides', async (req, res) => {
  const { riderId, pickup, dropoff } = req.body || {};
  if (!riderId || !pickup || !dropoff) {
    return res.status(400).json({ error: 'riderId, pickup and dropoff are required' });
  }
  const rideId = randomUUID();
  try {
    await createRide({ rideId, riderId, pickup, dropoff }); // saves REQUESTED + emits event
    await queue.add('offer', { rideId }, {
      jobId: rideId,                                  // dedupes: one job per ride
      attempts: 3,                                    // retry if the worker crashes / throws
      backoff: { type: 'exponential', delay: 500 },
      removeOnComplete: 1000,
      removeOnFail: 1000,
    });
  } catch (err) {
    console.error('booking failed', err);
    // never leave a ride dangling with no job behind it
    await transition(rideId, 'REQUESTED', 'NO_DRIVER_FOUND').catch(() => {});
    return res.status(503).json({ error: 'could not enqueue ride', rideId });
  }
  res.status(202).json({ rideId, status: 'REQUESTED' });
});

app.get('/rides/:id', async (req, res) => {
  const ride = await getRide(req.params.id);
  if (!ride) return res.status(404).json({ error: 'not found' });
  res.json(ride);
});

app.get('/health', (_req, res) => res.json({ ok: true }));

app.listen(PORT, () => console.log(`[api] listening on :${PORT}`));
