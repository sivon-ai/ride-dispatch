const { Queue } = require('bullmq');
const { make } = require('./redis');
const { QUEUE_NAME } = require('./config');

const queue = new Queue(QUEUE_NAME, { connection: make() });
module.exports = { queue };
