const express = require("express");
const http = require("http");
// const WebSocket = require("ws");
const axios = require("axios");
const { createProxyServer } = require("http-proxy");

const app = express();
const PORT = process.env.PORT;
const ROUTER_URL = process.env.ROUTER_URL;

const server = http.createServer(app);
const proxy = createProxyServer({ ws: true });
// const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", async (req, socket, head) => {
  console.log("upgrade request....");
  const url = new URL(req.url, `http://${req.headers.host}`);
  const user_id = url.searchParams.get("user_id");

  if (!user_id) {
    socket.destroy();
    return;
  }

  try {
    const { data } = await axios.get(`${ROUTER_URL}/route/user/${user_id}`);
    const targetNode = data.node;
    console.log(`[Gateway] Routing user ${user_id} to ${targetNode}`);

    proxy.ws(req, socket, head, { target: targetNode });
  } catch (err) {
    console.error("Routing error:", err.message);
    socket.destroy();
  }
});

app.get("/", (req, res) => {
  res.send("Welcome to gateway server");
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
