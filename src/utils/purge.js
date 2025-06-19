const { mysqlConn, pgClient } = require("../config/db");
const { log } = require("../utils/log");

async function purgeMySQLTables() {
  log("Deleting existing records from MySQL tables...");
  await mysqlConn.query("SET FOREIGN_KEY_CHECKS = 0");
  await mysqlConn.query("DELETE FROM MARKS");
  await mysqlConn.query("DELETE FROM SUBJECTS");
  await mysqlConn.query("DELETE FROM STUDENTS");
  await mysqlConn.query("DELETE FROM DEPARTMENTS");
  await mysqlConn.query("DELETE FROM GRADES");
  await mysqlConn.query("SET FOREIGN_KEY_CHECKS = 1");
  log("✅ MySQL tables cleared.");
}

async function purgePostgresTables() {
  log("Deleting existing records from PostgreSQL tables...");
  await pgClient.query("DELETE FROM student_academics");
  log("✅ PostgreSQL tables cleared.");
}

module.exports = {
  purgeMySQLTables,
  purgePostgresTables,
};
