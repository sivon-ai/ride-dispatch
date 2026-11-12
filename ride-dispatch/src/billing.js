// Program A (billing): reads every event, charges the rider when a ride is ASSIGNED.
const { runConsumer } = require('./consumer');

runConsumer({
  group: 'billing',
  name: `billing-${process.pid}`,
  onEvent: async (e) => {
    if (e.status === 'ASSIGNED') console.log(`charging rider for ride ${e.rideId}`);
  },
}).catch((e) => { console.error(e); process.exit(1); });
