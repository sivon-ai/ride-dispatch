// Program B (ops): reads every event, prints each status change.
const { runConsumer } = require('./consumer');

runConsumer({
  group: 'ops',
  name: `ops-${process.pid}`,
  onEvent: async (e) => console.log(`ride ${e.rideId} is now in status ${e.status}`),
}).catch((e) => { console.error(e); process.exit(1); });
