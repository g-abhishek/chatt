const { Kafka } = require("kafkajs");
const { getSocket, listUsers } = require("../connection");
const { assignedPartitions } = require("./node-partitions");
const GROUP_ID = `realtime-delivery`;
const MESSAGE_INBOX_TOPIC = process.env.MESSAGE_INBOX_TOPIC;
const brokers = ["localhost:9092"];

const kafka = new Kafka({
  clientId: "websocket-node",
  brokers,
});

const consumer = kafka.consumer({
  groupId: GROUP_ID,
});

const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: MESSAGE_INBOX_TOPIC,
    fromBeginning: true,
  });
  console.log(`Kafka consumer running in group ${GROUP_ID}`);

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    const assigned = payload.memberAssignment[MESSAGE_INBOX_TOPIC];

    console.log(`[WS Node ${process.env.PORT}] assigned partitions:`, assigned);

    if (Array.isArray(assigned)) {
      assignedPartitions.length = 0; // clear
      assigned.forEach((p) => assignedPartitions.push(p));
    }
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log("messages.inbox consumer data", { partition, message });
      const payload = message.value.toString();
      //   console.log(payload);
      //   console.log(JSON.parse(payload));
      const {
        from,
        to: user_id,
        message: msg,
        timestamp,
      } = JSON.parse(payload);
      console.log(from, user_id, msg, timestamp);

      // console.log(listUsers());
      const socket = getSocket(user_id);
      // console.log("socket>>>>", socket);
      if (socket) {
        socket.send(JSON.stringify({ from, message: msg, timestamp }));
      } else {
        console.log(`User ${user_id} is not connected on this node`);
        // Future: persist to DB for offline delivery
      }

      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset, 10) + 1).toString(),
        },
      ]);
    },
  });
};

module.exports = {
  startConsumer,
};
