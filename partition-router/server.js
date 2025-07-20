const { default: axios } = require("axios");
const express = require("express");

const app = express();
const PORT = process.env.PORT;

const totalPartitions = 4;
const websocketNodes = [
  { id: "node-1", url: "http://localhost:3000" },
  { id: "node-2", url: "http://localhost:4000" },
];
const partitionToNode = new Map();
// poll nodes every 5s
const pollWebSocketNodes = async () => {
  partitionToNode.clear(); // ← critical line

  for (const node of websocketNodes) {
    try {
      const res = await axios.get(`${node.url}/partitions`);
      const partitions = res.data.partitions || [];

      partitions.forEach((p) => partitionToNode.set(p, node.url));
      console.log(partitionToNode);
    } catch (err) {
      console.error(`Failed to reach ${node.id}:`, err.message);
    }
  }
};

// repoll periodically
setInterval(pollWebSocketNodes, 5000);
pollWebSocketNodes(); // initial

// route a user to their correct node
app.get("/route/user/:user_id", async (req, res) => {
  try {
    const { user_id } = req.params;

    const { data } = await axios.get(
      `http://localhost:3000/get-user-partition/${user_id}`
    );
    console.log(data);
    const { partition } = data;

    console.log(`Routing user ${user_id} to partition ${partition}`);

    const node = partitionToNode.get(partition);
    if (!node) {
      return res.status(503).send({ error: "Partition not assigned yet" });
    }

    console.log(
      `Partition router for user ${user_id} routed to ${node} on partition ${partition}`
    );
    return res.send({ node, partition });
  } catch (err) {
    console.log(err);
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
