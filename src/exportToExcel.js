const { pgClient } = require("./config/db");
const ExcelJS = require("exceljs");
const { log } = require("./utils/log");

async function exportStudentAcademicsToExcel() {
  try {
    log("🔌 Connecting to PostgreSQL for data export...", "info");
    await pgClient.connect();

    log("📥 Fetching data from PostgreSQL...", "info");
    const result = await pgClient.query(`
      SELECT
        student_academics_student_id AS student_id,
        student_academics_first_name AS first_name,
        student_academics_last_name AS last_name,
        student_academics_email AS email,
        student_academics_department AS department,
        student_academics_joining_date AS joining_date,
        student_academics_gpa AS gpa
      FROM student_academics
      ORDER BY student_academics_student_id
    `);

    const rows = result.rows;

    log(`📊 Retrieved ${rows.length} rows from student_academics.`, "info");

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Student Academics");

    worksheet.columns = [
      { header: "Student ID", key: "student_id", width: 15 },
      { header: "First Name", key: "first_name", width: 20 },
      { header: "Last Name", key: "last_name", width: 20 },
      { header: "Email", key: "email", width: 30 },
      { header: "Department", key: "department", width: 20 },
      { header: "Joining Date", key: "joining_date", width: 20 },
      { header: "GPA", key: "gpa", width: 10 },
    ];

    rows.forEach((row) => {
      worksheet.addRow(row);
    });

    worksheet.getRow(1).font = { bold: true };

    await workbook.xlsx.writeFile("student_academics.xlsx");
    log("✅ Excel file 'student_academics.xlsx' has been created.", "success");
  } catch (err) {
    log(`❌ Error exporting to Excel: ${err.message}`, "error");
  } finally {
    try {
      await pgClient.end();
      log("🔌 PostgreSQL connection closed after export.", "info");
    } catch (closeErr) {
      log(
        `⚠️ Failed to close PostgreSQL connection: ${closeErr.message}`,
        "warn"
      );
    }
  }
}

exportStudentAcademicsToExcel();
