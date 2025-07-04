# ETL Assignment – MySQL to PostgreSQL

## 📦 Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Node.js. It reads raw student and grade data from CSV files, loads it into a MySQL database, transforms it by calculating GPA, and then loads the result into a PostgreSQL data warehouse.

---

## 🛠️ Setup Instructions

### ✅ Prerequisites

Ensure the following are installed:

- Node.js and npm
- MySQL Server & MySQL CLI
- PostgreSQL & psql CLI
- MySQL and PostgreSQL credentials configured
- Add MySQL and PostgreSQL `bin` folders to system PATH

---

## ⚙️ Environment Variables

Create a `.env` file in the root with the following content:

```env
# MySQL
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=etl_source

# PostgreSQL
PGHOST=localhost
PGUSER=postgres
PGPASSWORD=your_pg_password
PGDATABASE=etl_target
PGPORT=5432

## 🚀 Run Project Locally

Follow these steps to run the ETL pipeline on your local machine.

---

1. Install Dependencies
npm install

2. Create Databases
MySQL:
mysql -u root -p -e "CREATE DATABASE etl_source;"
PostgreSQL:
createdb -U postgres etl_target

3. Create Tables
MySQL:
Get-Content scripts\mysql\create_mysql_tables.sql | mysql -u root -p etl_source
PostgreSQL:
psql -U postgres -d etl_target -f scripts/postgres/create_postgres_tables.sql

4. Load Data into MySQL
node src/load_to_mysql

5. Load Transformed Data into PostgreSQL
node src/mysql_to_postgres

After running the pipeline, the student_academics table in PostgreSQL will contain student information with calculated GPA based on average marks.

You can verify using:
SELECT * FROM student_academics;
