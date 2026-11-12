const IORedis = require('ioredis');
const { REDIS_URL } = require('./config');

// BullMQ workers/blocking readers need maxRetriesPerRequest: null
const make = () => new IORedis(REDIS_URL, { maxRetriesPerRequest: null });
module.exports = { make };
