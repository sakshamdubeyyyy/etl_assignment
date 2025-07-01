const fs = require("fs");
const csv = require("csv-parser");
const { mysqlConn } = require("./config/db");
const { log } = require("./utils/log");

function validateRow(row, requiredFields) {
  for (const field of requiredFields) {
    if (!row[field] || row[field].trim?.() === "") {
      throw new Error(`Missing required field: ${field}`);
    }
  }
}

function validateFile(filePath, allowedExt = ".txt") {
  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }
  if (!filePath.endsWith(allowedExt)) {
    throw new Error(`Invalid file format. Expected a ${allowedExt} file.`);
  }
}

async function loadGrades() {
  const filePath = "./data/grade.txt";
  validateFile(filePath);

  const rows = [];
  log("📘 Starting to load grade information...", "info");

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      try {
        for (const row of rows) {
          validateRow(row, [
            "grade_id",
            "grade_code",
            "grade_label",
            "percentage_range",
            "gpa_equivalent",
          ]);

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
        }
        log(`✅ All ${rows.length} grades have been processed.`, "success");
      } catch (err) {
        log(`❌ Grade processing failed: ${err.message}`, "error");
        process.exit(1);
      }
    });
}
async function loadStudents() {
  const filePath = "./data/students.txt";
  validateFile(filePath);

  const rows = [];
  const departmentCache = {}; 
  log("👥 Starting to load student data...", "info");

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      try {
        for (const row of rows) {
          validateRow(row, [
            "student_id",
            "first_name",
            "last_name",
            "email",
            "department",
            "joining_date",
          ]);

          const deptName = row.department.trim();
          let dept_id;

          if (departmentCache[deptName]) {
            dept_id = departmentCache[deptName];
          } else {
            await mysqlConn.query(
              "INSERT IGNORE INTO DEPARTMENTS(DEPARTMENTS_NAME) VALUES (?)",
              [deptName]
            );

            const [deptIdRow] = await mysqlConn.query(
              "SELECT DEPARTMENTS_DEPT_ID FROM DEPARTMENTS WHERE DEPARTMENTS_NAME = ?",
              [deptName]
            );

            dept_id = deptIdRow[0]?.DEPARTMENTS_DEPT_ID;
            if (!dept_id)
              throw new Error(`Department ID not found for: ${deptName}`);

            departmentCache[deptName] = dept_id;
            log(`🏢 Department ensured → ${deptName}`, "info");
          }

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
            log(`📘 Subject ensured → ${subject}`, "info");

            const [subjectIdRow] = await mysqlConn.query(
              "SELECT SUBJECT_ID FROM SUBJECTS WHERE SUBJECT_NAME = ?",
              [subject]
            );
            const subject_id = subjectIdRow[0]?.SUBJECT_ID;
            if (!subject_id)
              throw new Error(`Subject ID not found for: ${subject}`);

            await mysqlConn.query(
              "INSERT INTO MARKS(MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS_SCORED) VALUES (?, ?, ?)",
              [row.student_id, subject_id, marks]
            );
            log(
              `✏️ Mark added → ${subject}: ${marks} (Student ID: ${row.student_id})`,
              "success"
            );
          }
        }

        log(
          `✅ Successfully processed ${rows.length} student records.`,
          "success"
        );
      } catch (err) {
        log(`❌ Student data load failed: ${err.message}`, "error");
        process.exit(1);
      }
    });
}

(async () => {
  try {
    await loadGrades();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await loadStudents();
  } catch (err) {
    log(`❌ Unexpected error during MySQL ETL: ${err.message}`, "error");
    process.exit(1);
  }
})();
