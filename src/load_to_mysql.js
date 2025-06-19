const fs = require("fs");
const csv = require("csv-parser");
const { mysqlConn } = require("./config/db");
const { log } = require("./utils/log");
const { purgeMySQLTables } = require("./utils/purge");

async function loadGrades() {
  const rows = [];
  log("Starting to load grades...");
  fs.createReadStream("./data/grade.txt")
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const row of rows) {
        await mysqlConn.query("INSERT INTO GRADES VALUES (?, ?, ?, ?, ?)", [
          row.grade_id,
          row.grade_code,
          row.grade_label,
          row.percentage_range,
          row.gpa_equivalent,
        ]);
        log(`Inserted grade: ${JSON.stringify(row)}`);
      }
      log("✅ Grades loaded.");
    });
}

async function loadStudents() {
  const rows = [];
  log("Starting to load students...");
  fs.createReadStream("./data/students.txt")
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const row of rows) {
        try {
          await mysqlConn.query(
            "INSERT IGNORE INTO DEPARTMENTS(DEPARTMENTS_NAME) VALUES (?)",
            [row.department.trim()]
          );
          log(`Inserted department: ${row.department.trim()}`);

          const [deptIdRow] = await mysqlConn.query(
            "SELECT DEPARTMENTS_DEPT_ID FROM DEPARTMENTS WHERE DEPARTMENTS_NAME = ?",
            [row.department.trim()]
          );
          const dept_id = deptIdRow[0].DEPARTMENTS_DEPT_ID;

          await mysqlConn.query(
            "INSERT INTO STUDENTS VALUES (?, ?, ?, ?, ?, ?)",
            [
              row.student_id,
              row.first_name,
              row.last_name,
              row.email,
              dept_id,
              row.joining_date,
            ]
          );
          log(`Inserted student: ${row.student_id}`);

          for (let i = 1; i <= 5; i++) {
            const subject = row[`subject${i}`]?.trim();
            const marks = parseInt(
              row[`subject${i}_marks`] || row[`subject${i}_mark`]
            );

            if (!subject || isNaN(marks)) continue;

            await mysqlConn.query(
              "INSERT IGNORE INTO SUBJECTS(SUBJECT_NAME, SUBJECT_DEPT_ID) VALUES (?, ?)",
              [subject, dept_id]
            );
            log(`Inserted subject: ${subject}`);

            const [subjectIdRow] = await mysqlConn.query(
              "SELECT SUBJECT_ID FROM SUBJECTS WHERE SUBJECT_NAME = ?",
              [subject]
            );
            const subject_id = subjectIdRow[0].SUBJECT_ID;

            await mysqlConn.query(
              "INSERT INTO MARKS(MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS_SCORED) VALUES (?, ?, ?)",
              [row.student_id, subject_id, marks]
            );
            log(
              `Inserted mark: student_id=${row.student_id}, subject=${subject}, marks=${marks}`
            );
          }
        } catch (err) {
          log(`❌ Error processing student ${row.student_id}: ${err.message}`);
        }
      }

      log("✅ Students, Departments, Subjects, and Marks loaded.");
    });
}

(async () => {
  try {
    await purgeMySQLTables();
    await loadGrades();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await loadStudents();
  } catch (err) {
    log(`❌ Unexpected error: ${err.message}`);
  }
})();
