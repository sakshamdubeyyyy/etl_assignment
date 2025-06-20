const fs = require("fs");
const csv = require("csv-parser");
const { mysqlConn } = require("./config/db");
const { log } = require("./utils/log");
const { purgeMySQLTables } = require("./utils/purge");

async function loadGrades() {
  const rows = [];
  log("📘 Starting to load grade information...", "info");

  fs.createReadStream("./data/grade.txt")
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const row of rows) {
        try {
          await mysqlConn.query("INSERT INTO GRADES VALUES (?, ?, ?, ?, ?)", [
            row.grade_id,
            row.grade_code,
            row.grade_label,
            row.percentage_range,
            row.gpa_equivalent,
          ]);
          log(
            `📄 Grade inserted → ${row.grade_label} (${row.grade_code})`,
            "success"
          );
        } catch (err) {
          log(
            `❌ Failed to insert grade ${row.grade_code}: ${err.message}`,
            "error"
          );
        }
      }
      log(`✅ All ${rows.length} grades have been processed.`, "success");
    });
}

async function loadStudents() {
  const rows = [];
  log("👥 Starting to load student data...", "info");

  fs.createReadStream("./data/students.txt")
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const row of rows) {
        try {
          const deptName = row.department.trim();
          try {
            await mysqlConn.query(
              "INSERT IGNORE INTO DEPARTMENTS(DEPARTMENTS_NAME) VALUES (?)",
              [deptName]
            );
            log(`🏢 Department ensured → ${deptName}`, "info");
          } catch (err) {
            log(
              `❌ Failed to insert department ${deptName}: ${err.message}`,
              "error"
            );
          }

          let dept_id;
          try {
            const [deptIdRow] = await mysqlConn.query(
              "SELECT DEPARTMENTS_DEPT_ID FROM DEPARTMENTS WHERE DEPARTMENTS_NAME = ?",
              [deptName]
            );
            dept_id = deptIdRow[0]?.DEPARTMENTS_DEPT_ID;
            if (!dept_id) throw new Error("Department ID not found");
          } catch (err) {
            log(
              `❌ Failed to fetch department ID for ${deptName}: ${err.message}`,
              "error"
            );
            continue;
          }

          try {
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
            log(
              `🧑‍🎓 Student added → ${row.first_name} ${row.last_name} (ID: ${row.student_id})`,
              "success"
            );
          } catch (err) {
            log(
              `❌ Failed to insert student ${row.student_id}: ${err.message}`,
              "error"
            );
            continue;
          }

          for (let i = 1; i <= 5; i++) {
            const subject = row[`subject${i}`]?.trim();
            const marks = parseInt(
              row[`subject${i}_marks`] || row[`subject${i}_mark`]
            );

            if (!subject || isNaN(marks)) continue;

            try {
              await mysqlConn.query(
                "INSERT IGNORE INTO SUBJECTS(SUBJECT_NAME, SUBJECT_DEPT_ID) VALUES (?, ?)",
                [subject, dept_id]
              );
              log(`📘 Subject ensured → ${subject}`, "info");
            } catch (err) {
              log(
                `❌ Failed to insert subject ${subject}: ${err.message}`,
                "error"
              );
              continue;
            }

            let subject_id;
            try {
              const [subjectIdRow] = await mysqlConn.query(
                "SELECT SUBJECT_ID FROM SUBJECTS WHERE SUBJECT_NAME = ?",
                [subject]
              );
              subject_id = subjectIdRow[0]?.SUBJECT_ID;
              if (!subject_id) throw new Error("Subject ID not found");
            } catch (err) {
              log(
                `❌ Failed to fetch subject ID for ${subject}: ${err.message}`,
                "error"
              );
              continue;
            }

            try {
              await mysqlConn.query(
                "INSERT INTO MARKS(MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS_SCORED) VALUES (?, ?, ?)",
                [row.student_id, subject_id, marks]
              );
              log(
                `✏️ Mark added → ${subject}: ${marks} (Student ID: ${row.student_id})`,
                "success"
              );
            } catch (err) {
              log(
                `❌ Failed to insert marks for ${subject} (Student ID: ${row.student_id}): ${err.message}`,
                "error"
              );
            }
          }
        } catch (err) {
          log(
            `❌ Unexpected error while processing student ID ${row.student_id}: ${err.message}`,
            "error"
          );
        }
      }

      log(
        `✅ Successfully processed ${rows.length} student records.`,
        "success"
      );
    });
}

(async () => {
  try {
    log("🧹 Clearing existing MySQL tables...", "info");
    await purgeMySQLTables();
    log("✅ Tables cleared successfully.", "success");

    await loadGrades();
    await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait for grade loading
    await loadStudents();
  } catch (err) {
    log(`❌ Unexpected error occurred during ETL: ${err.message}`, "error");
  }
})();
