const { mysqlConn, pgClient } = require("../config/db");
const { log } = require("./log");

async function purgeMySQLTables() {
  try {
    log("🧹 Deleting existing records from MySQL tables...");
    await mysqlConn.query("SET FOREIGN_KEY_CHECKS = 0");
    await mysqlConn.query("DELETE FROM MARKS");
    await mysqlConn.query("DELETE FROM SUBJECTS");
    await mysqlConn.query("DELETE FROM STUDENTS");
    await mysqlConn.query("DELETE FROM DEPARTMENTS");
    await mysqlConn.query("DELETE FROM GRADES");   
    await mysqlConn.query("SET FOREIGN_KEY_CHECKS = 1");
    log("✅ MySQL tables cleared.");
  } catch (err) {
    log(`❌ Error while purging MySQL: ${err.message}`, "error");
  }
}

async function purgePostgresTables() {
  try {
    log("🧹 Attempting to connect to PostgreSQL...");
    await pgClient.connect();
    log("🔌 Connected to PostgreSQL.");

    log(
      "🧹 Deleting existing records from PostgreSQL table `student_academics`..."
    );
    const res = await pgClient.query("DELETE FROM student_academics");
    log(`✅ PostgreSQL table cleared. Rows affected: ${res.rowCount}`);
  } catch (err) {
    log(`❌ Error while purging PostgreSQL: ${err.message}`, "error");
  } finally {
    try {
      await pgClient.end();
      log("🔌 PostgreSQL connection closed.");
    } catch (err) {
      log(`⚠️ Error closing PostgreSQL connection: ${err.message}`, "warn");
    }
  }
}

(async () => {
  try {
    await purgeMySQLTables();
    await purgePostgresTables();
  } catch (err) {
    log(`❌ Purge script failed: ${err.message}`, "error");
    process.exit(1);
  } finally {
    try {
      await mysqlConn.end();
    } catch (mysqlErr) {
      log(`⚠️ Error closing MySQL connection: ${mysqlErr.message}`, "warn");
    }
  }
})();
