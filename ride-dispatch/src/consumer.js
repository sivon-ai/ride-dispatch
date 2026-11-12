// Shared Redis Streams consumer-group runner.
// Each program uses its OWN group name, so each group receives EVERY event
// (fan-out). Consumers inside the same group would split the events instead.
const { make } = require('./redis');
const { STREAM } = require('./config');

async function runConsumer({ group, name, onEvent }) {
  const redis = make();
  try {
    // '0' = start from the beginning, so late-starting consumers miss nothing
    await redis.xgroup('CREATE', STREAM, group, '0', 'MKSTREAM');
  } catch (e) {
    if (!String(e.message).includes('BUSYGROUP')) throw e;
  }
  console.log(`[${group}] consuming "${STREAM}" as ${name}`);

  let id = '0'; // first drain our own un-acked backlog (crash recovery), then switch to '>'
  for (;;) {
    const res = await redis.xreadgroup('GROUP', group, name, 'COUNT', 50, 'BLOCK', 2000, 'STREAMS', STREAM, id);
    if (!res) { if (id === '0') id = '>'; continue; }
    const entries = res[0][1];
    if (id === '0' && entries.length === 0) { id = '>'; continue; }

    for (const [entryId, fields] of entries) {
      const evt = {};
      for (let i = 0; i < fields.length; i += 2) evt[fields[i]] = fields[i + 1];
      // SADD returns 1 only for first sight -> exact "events seen" count even
      // if an event is redelivered after a crash. Used by the load test.
      await redis.sadd(`seen:${group}`, entryId);
      await onEvent(evt);
      await redis.xack(STREAM, group, entryId);
    }
  }
}

module.exports = { runConsumer };
