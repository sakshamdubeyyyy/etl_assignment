const { mysqlConn, pgClient } = require("./config/db");
const mapToGPA = require("./utils/gpaMapper");
const { log } = require("./utils/log");
const { purgePostgresTables } = require("./utils/purge");

(async () => {
  try {
    log("🔌 Connecting to PostgreSQL...", "info");
    await pgClient.connect();
    log("✅ Successfully connected to PostgreSQL.", "success");

    log("🧹 Clearing existing PostgreSQL data...", "info");
    await purgePostgresTables();
    log("✅ PostgreSQL tables purged.", "success");

    log("🚀 Starting data transfer from MySQL to PostgreSQL...", "info");

    let rows;
    try {
      const [result] = await mysqlConn.query(`
        SELECT 
          s.STUDENT_ID AS student_id,
          s.STUDENTS_FIRST_NAME AS first_name,
          s.STUDENTS_LAST_NAME AS last_name,
          s.STUDENTS_EMAIL AS email,
          d.DEPARTMENTS_NAME AS department,
          s.STUDENTS_JOINING_DATE AS joining_date,
          AVG(m.MARKS_SCORED) AS avg_marks
        FROM STUDENTS s
        JOIN DEPARTMENTS d ON s.STUDENTS_DEPT_ID = d.DEPARTMENTS_DEPT_ID
        JOIN MARKS m ON s.STUDENT_ID = m.MARKS_STUDENT_ID
        GROUP BY s.STUDENT_ID
      `);
      rows = result;
      log(`📊 Retrieved ${rows.length} student records from MySQL.`, "info");
    } catch (mysqlErr) {
      log(`❌ Error fetching data from MySQL: ${mysqlErr.message}`, "error");
      return;
    }

    for (const row of rows) {
      const gpa = mapToGPA(row.avg_marks);

      try {
        await pgClient.query(
          `INSERT INTO student_academics 
           (student_academics_student_id, student_academics_first_name, student_academics_last_name, 
            student_academics_email, student_academics_department, student_academics_joining_date, 
            student_academics_gpa)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (student_academics_student_id) DO NOTHING`,
          [
            row.student_id,
            row.first_name,
            row.last_name,
            row.email,
            row.department,
            row.joining_date,
            gpa,
          ]
        );
        log(
          `📦 Transferred → ${row.first_name} ${row.last_name} (ID: ${row.student_id}) | GPA: ${gpa}`,
          "success"
        );
      } catch (pgErr) {
        log(
          `❌ Failed to insert student ID ${row.student_id} into PostgreSQL: ${pgErr.message}`,
          "error"
        );
      }
    }

    log(
      `✅ All ${rows.length} student records processed for PostgreSQL.`,
      "success"
    );
  } catch (err) {
    log(`❌ Unexpected error in transfer process: ${err.message}`, "error");
  } finally {
    try {
      await pgClient.end();
      log("🔌 PostgreSQL connection closed.", "info");
    } catch (closeErr) {
      log(
        `⚠️ Failed to close PostgreSQL connection: ${closeErr.message}`,
        "warn"
      );
    }
  }
})();
