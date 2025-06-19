const path = require("path");

const logFilePath = path.join(__dirname, "etl_logs.txt");

function log(message) {
  const timestamp = new Date().toISOString();
  const fullMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(logFilePath, fullMessage);
  console.log(fullMessage.trim());
}

module.exports = {log};