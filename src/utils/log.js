const path = require("path");
const fs = require("fs");

function getDateString() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function log(message, type = "info") {
  const timestamp = new Date().toLocaleString();
  const dateString = getDateString();
  const logDir = path.join(__dirname, "logs"); 
  const logFilePath = path.join(logDir, `etl_logs_${dateString}.txt`);

  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir); 
  }

  let symbol, label;

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
