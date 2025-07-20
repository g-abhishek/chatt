const connectedUsers = new Map();

function addUser(userId, socket) {
  return connectedUsers.set(userId, socket);
}
function removeUser(userId) {
  return connectedUsers.delete(userId);
}
function getSocket(userId) {
  return connectedUsers.get(userId);
}
function isConnected(userId, socket) {
  return connectedUsers.has(userId);
}
function listUsers(userId, socket) {
  return [...connectedUsers.keys()];
}

module.exports = {
  addUser,
  removeUser,
  getSocket,
  isConnected,
  listUsers,
};
