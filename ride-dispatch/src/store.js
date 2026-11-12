// Ride state lives in Redis. Every state change is a Lua script, so the
// "compare status + update + publish event" step is ATOMIC:
//   - a ride can only move REQUESTED -> ASSIGNED / NO_DRIVER_FOUND once
//   - a status change and its event can never get out of sync (no lost events)
const { make } = require('./redis');
const { STREAM } = require('./config');

const redis = make();

const rideKey = (id) => `ride:${id}`;
const assignKey = (id) => `ride:${id}:assignments`;

redis.defineCommand('createRide', {
  numberOfKeys: 2, // ride hash, stream
  lua: `
    if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
    redis.call('HSET', KEYS[1], 'rideId', ARGV[1], 'riderId', ARGV[2], 'pickup', ARGV[3],
      'dropoff', ARGV[4], 'status', 'REQUESTED', 'driverId', '', 'createdAt', ARGV[5], 'updatedAt', ARGV[5])
    redis.call('XADD', KEYS[2], '*', 'rideId', ARGV[1], 'status', 'REQUESTED', 'driverId', '', 'ts', ARGV[5])
    return 1
  `,
});

redis.defineCommand('transitionRide', {
  numberOfKeys: 3, // ride hash, assignments list, stream
  lua: `
    local cur = redis.call('HGET', KEYS[1], 'status')
    if cur ~= ARGV[1] then return 0 end            -- compare-and-set guard
    redis.call('HSET', KEYS[1], 'status', ARGV[2], 'updatedAt', ARGV[4])
    if ARGV[3] ~= '' then
      redis.call('HSET', KEYS[1], 'driverId', ARGV[3])
      redis.call('RPUSH', KEYS[2], ARGV[3])        -- audit trail: who got assigned
    end
    redis.call('XADD', KEYS[3], '*', 'rideId', ARGV[5], 'status', ARGV[2], 'driverId', ARGV[3], 'ts', ARGV[4])
    return 1
  `,
});

async function createRide({ rideId, riderId, pickup, dropoff }) {
  const ok = await redis.createRide(rideKey(rideId), STREAM, rideId, riderId, pickup, dropoff, Date.now());
  return ok === 1;
}

// returns true only if THIS call performed the transition
async function transition(rideId, from, to, driverId = '') {
  const ok = await redis.transitionRide(
    rideKey(rideId), assignKey(rideId), STREAM, from, to, driverId, Date.now(), rideId,
  );
  return ok === 1;
}

async function getRide(rideId) {
  const ride = await redis.hgetall(rideKey(rideId));
  if (!ride.rideId) return null;
  ride.assignments = await redis.lrange(assignKey(rideId), 0, -1);
  return ride;
}

module.exports = { createRide, transition, getRide, redis };
