const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { addUser, removeUser, getSocket } = require("./connection");
const { sendMessageToKafka, startProducer } = require("./kafka/producer");
const { startConsumer } = require("./kafka/consumer");
const {
  assignedPartitions,
  getKafkaPartition,
} = require("./kafka/node-partitions");
const PORT = process.env.PORT;

const app = express();
const httpServer = http.createServer(app);
const wss = new WebSocket.Server({ server: httpServer });

startProducer();
startConsumer();

app.use(express.static("./"));

// expose assigned partitions via HTTP
app.get("/partitions", (req, res) => {
  res.send({ partitions: assignedPartitions });
});

app.get("/get-user-partition/:user_id", (req, res) => {
  const { user_id } = req.params;
  const partition = getKafkaPartition(user_id, 4);
  console.log(`User ID: ${user_id}, Partition: ${partition}`);

  res.send({ user_id, partition });
});

httpServer.listen(PORT, () => {
  console.log("Server is running on port", PORT);
});

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const user_id = url.searchParams.get("user_id");

  if (!user_id) {
    ws.close(1008, "Missing user_id");
    return;
  }

  console.log(`User connected: ${user_id}`);
  addUser(user_id, ws);

  ws.on("message", async (raw) => {
    try {
      let data = raw;
      if (Buffer.isBuffer(raw)) {
        data = JSON.parse(raw.toString()); // Convert to string
      }

      if (data.type === "send_message") {
        const { to, message } = data;
        console.log(`[${user_id}] → [${to}] | ${message}`);

        await sendMessageToKafka({ from: user_id, to, message });
      }

      // you can handle other types too:
      // if (data.type === "join_room") { ... }
    } catch (e) {
      console.error("Invalid message:", raw, e);
    }
  });

  ws.on("close", () => {
    console.log(`User disconnected: ${user_id}`);
    removeUser(user_id);
  });
});
