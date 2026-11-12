module.exports = {
  REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  PORT: Number(process.env.PORT || 3000),
  QUEUE_NAME: 'ride-offers',
  STREAM: 'ride-events',
  MAX_OFFERS: 3,                                   // drivers tried before giving up
  ACCEPT_RATE: Number(process.env.ACCEPT_RATE || 0.5),
  WORKER_CONCURRENCY: Number(process.env.WORKER_CONCURRENCY || 20),
  // 10 hardcoded fake drivers
  DRIVERS: Array.from({ length: 10 }, (_, i) => `driver-${String(i + 1).padStart(2, '0')}`),
};
