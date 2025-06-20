const path = require("path");
const fs = require("fs");
const logFilePath = path.join(__dirname, "etl_logs.txt");

function log(message, type = "info") {
  const timestamp = new Date().toLocaleString(); // Friendlier format
  let symbol;
  let label;

  switch (type) {
    case "success":
      symbol = "✅";
      label = "SUCCESS";
      break;
    case "error":
      symbol = "❌";
      label = "ERROR";
      break;
    case "warn":
      symbol = "⚠️";
      label = "WARNING";
      break;
    case "info":
    default:
      symbol = "ℹ️";
      label = "INFO";
      break;
  }

  const fullMessage = `${symbol} [${timestamp}] [${label}] ${message}\n`;

  fs.appendFileSync(logFilePath, fullMessage);
  console.log(fullMessage.trim());
}

module.exports = { log };
