const { Partitioners } = require("kafkajs");
const assignedPartitions = [];
const murmurhash = require('murmurhash')


const getKafkaPartition = (key, totalPartitions = 4) => {
  if (!key) return null; // fallback for null keys

  const keyBuffer = Buffer.from(key); // must be buffer
  const hash = murmurhash.v2(keyBuffer);    // Kafka-compatible Murmur2
  return Math.abs(hash) % totalPartitions;
};



module.exports = {
  assignedPartitions,
  getKafkaPartition,
};
