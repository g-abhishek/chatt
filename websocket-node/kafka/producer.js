const { Kafka } = require("kafkajs");
const brokers = ["localhost:9092"];

const kafka = new Kafka({
  clientId: "websocket-node",
  brokers,
});

const producer = kafka.producer();

const startProducer = async () => {
  producer.on(producer.events.CONNECT, (e) => {
    console.log(`Kafka producer connected ${e}`);
  });

  producer.on(producer.events.DISCONNECT, (e) => {
    console.log(`Kafka producer disconnected ${e}`);
  });

  await producer.connect();
};

const sendMessageToKafka = async ({ from, to, message }) => {
  console.log("Message sent:", { from, to, message });
  await producer.send({
    topic: process.env.MESSAGE_INBOX_TOPIC,
    messages: [
      {
        key: to, // ensures partition is stable per receiver, same partition for same user_id
        value: JSON.stringify({ from, to, message, timestamp: Date.now() }),
      },
    ],
  });
};

module.exports = {
  startProducer,
  sendMessageToKafka,
};
