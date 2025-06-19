const { mysqlConn, pgClient } = require("./config/db");
const mapToGPA = require("./utils/gpaMapper");

(async () => {
  try {
    await pgClient.connect();

    const [rows] = await mysqlConn.query(`
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

    for (const row of rows) {
      const gpa = mapToGPA(row.avg_marks);

      await pgClient.query(
        `INSERT INTO student_academics 
          (student_academics_student_id, student_academics_first_name, student_academics_last_name, student_academics_email, student_academics_department, student_academics_joining_date, student_academics_gpa)
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
    }

    console.log("✅ Data transferred from MySQL to PostgreSQL.");
  } catch (err) {
    console.error("❌ ETL Failed:", err.message);
  } finally {
    await pgClient.end();
  }
})();
