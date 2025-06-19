const fs = require("fs");
const csv = require("csv-parser");
const { mysqlConn } = require("./config/db");

async function loadGrades() {
  const rows = [];
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
      }
      console.log("✅ Grades loaded.");
    });
}

async function loadStudents() {
  const rows = [];
  fs.createReadStream("./data/students.txt")
    .pipe(csv())
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      for (const row of rows) {
        try {
          // 1. Insert Department
          await mysqlConn.query(
            "INSERT IGNORE INTO DEPARTMENTS(DEPARTMENTS_NAME) VALUES (?)",
            [row.department.trim()]
          );
          const [deptIdRow] = await mysqlConn.query(
            "SELECT DEPARTMENTS_DEPT_ID FROM DEPARTMENTS WHERE DEPARTMENTS_NAME = ?",
            [row.department.trim()]
          );
          const dept_id = deptIdRow[0].DEPARTMENTS_DEPT_ID;

          // 2. Insert Student
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

          // 3. Insert Subjects and Marks
          for (let i = 1; i <= 5; i++) {
            const subject = row[`subject${i}`]?.trim();
            const marks = parseInt(
              row[`subject${i}_marks`] || row[`subject${i}_mark`]
            );

            if (!subject || isNaN(marks)) continue;

            // Insert subject
            await mysqlConn.query(
              "INSERT IGNORE INTO SUBJECTS(SUBJECT_NAME, SUBJECT_DEPT_ID) VALUES (?, ?)",
              [subject, dept_id]
            );

            const [subjectIdRow] = await mysqlConn.query(
              "SELECT SUBJECT_ID FROM SUBJECTS WHERE SUBJECT_NAME = ?",
              [subject]
            );
            const subject_id = subjectIdRow[0].SUBJECT_ID;

            // Insert mark
            await mysqlConn.query(
              "INSERT INTO MARKS(MARKS_STUDENT_ID, MARKS_SUBJECT_ID, MARKS_SCORED) VALUES (?, ?, ?)",
              [row.student_id, subject_id, marks]
            );
          }
        } catch (err) {
          console.error(
            `❌ Error processing student ${row.student_id}:`,
            err.message
          );
        }
      }

      console.log("✅ Students, Departments, Subjects, and Marks loaded.");
    });
}

// Run both loaders
loadGrades().then(loadStudents).catch(console.error);
