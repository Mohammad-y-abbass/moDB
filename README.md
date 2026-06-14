# moDB: A Database Built from Scratch

## Overview

**moDB** is a persistent, SQL-compatible relational database management system (RDBMS) written in Go.

Unlike many simple data stores that operate entirely in memory, **moDB** is designed to mimic the architecture of production-grade databases like PostgreSQL or SQLite, persisting data to disk using a custom storage engine.

## 🎯 The Purpose

The primary goal of this project is **educational**. Building a database from scratch is one of the best ways to understand the complex systems that power the modern web. This project explores:

- **How SQL works** — How text-based queries are converted into machine-executable plans.
- **How data is stored** — How data is physically laid out on a hard drive using pages and slots.
- **How integrity is maintained** — How to maintain data integrity and persistence even when the server restarts.

By studying or contributing to moDB, you'll gain hands-on insight into:
- Lexical analysis & parsing (how raw text becomes structured commands)
- Query planning & optimization (even simple plans teach a lot)
- Storage engines (fixed-size pages, slotted-page architecture, serialization)
- Network protocols (TCP-based client/server interaction)
- ACID properties and why they matter

---

## 🏗 Architecture

moDB is divided into several clear layers that follow a traditional database pipeline:

### 1. Transport Layer (The Server)

A TCP server running on port `3003`. It handles multiple connections concurrently and provides a command-line interface (CLI) for users to interact with.

### 2. SQL Layer (The Brain)

- **Lexer**: Breaks down SQL strings into "tokens" (keywords, identifiers, numbers, operators).
- **Parser**: A recursive descent parser that validates syntax and builds an **Abstract Syntax Tree (AST)**.
- **Planner**: Converts the AST into a logical "Execution Plan" (e.g., Scan this table → Filter these rows → Project these columns).

### 3. Execution Layer

The **Executor** takes the plan and interacts with the storage engine to retrieve or modify data. It handles type conversions, null values, constraint checking (UNIQUE, NOT NULL, FOREIGN KEY), and FK cascade protection.

### 4. Storage Engine (The Heart)

- **Pager**: Data is split into fixed-size **4KB Pages**. This is the unit of communication between the DB and the Disk.
- **Slotted Pages**: Inside each page, we use a "Slotted Page Architecture" which allows us to manage rows efficiently and handle deletions without leaving wasteful gaps.
- **Persistence**: Data is saved in binary `.db` files, while table metadata (schemas) is persisted in `.json` files.

---

## ✅ Current SQL Features

### Data Definition Language (DDL)
- [x] **`CREATE DATABASE <name>`** — Multi-database support with directory-based isolation.
- [x] **`USE <name>`** — Switch between databases.
- [x] **`DROP DATABASE <name>`** — Delete an entire database (must be active first).
- [x] **`CREATE TABLE <name> (...)`** — Persistent schema definitions with support for `INT`, `TEXT`/`VARCHAR`, `NOT NULL`, `UNIQUE`, `PRIMARY KEY`, `REFERENCES` (Foreign Keys), and `FOREIGN KEY` constraints.
- [x] **`DROP TABLE <name>`** — Delete a table and all its data.

### Data Manipulation Language (DML)
- [x] **`INSERT INTO <table> [(cols)] VALUES (...)`** — Supports both positional and explicitly named column inserts.
- [x] **`SELECT [cols | *] FROM <table> [JOIN <table> ON <condition>] [WHERE <condition>]`** — Full table scans with `WHERE` filtering, column projection, and `INNER JOIN` support (Nested Loop Join).
- [x] **`UPDATE <table> SET col=val [, ...] [WHERE <condition>]`** — In-place updates with FK constraint validation.
- [x] **`DELETE FROM <table> [WHERE <condition>]`** — Physical deletion with orphan/cascade protection for FK references.

### Utility / Meta Commands
- [x] **`SHOW DATABASES`** — List all databases on the server.
- [x] **`SHOW TABLES`** — List all tables in the active database.

### Constraints & Integrity
- [x] **NOT NULL** — Enforced at insert and update time.
- [x] **UNIQUE** — Enforced at insert and update time.
- [x] **PRIMARY KEY** — Enforced via NOT NULL + UNIQUE.
- [x] **FOREIGN KEY / REFERENCES** — Full referential integrity: blocks invalid inserts, updates, and cascading-or-phaned deletes.
- [x] **Persistence** — All data remains intact after terminal/server restarts.

---

## 🗺 Roadmap: Future Features

### Performance & Indexing
- [ ] **B+ Tree Indexing** — Moving from O(n) full table scans to O(log n) searches for Primary Keys.
- [ ] **Buffer Pool Manager** — A cache to keep frequently used pages in RAM.

### Advanced SQL
- [ ] **Scalar Functions**: `UPPER()`, `LOWER()`, `LENGTH()`.
- [ ] **Aggregate Functions**: `COUNT()`, `SUM()`, `AVG()`.
- [ ] **GROUP BY & HAVING** — For complex data analysis.
- [x] **ORDER BY / LIMIT / OFFSET** — Sorted and paginated results.
- [x] **LIKE / IN / BETWEEN / IS NULL** — Advanced WHERE operators.
- [x] **DISTINCT** — Deduplicate result rows.
- [ ] **Subqueries / Nested SELECT**.

### Reliability (ACID)
- [ ] **Transactions**: `BEGIN`, `COMMIT`, and `ROLLBACK` to ensure atomic operations.
- [ ] **WAL (Write-Ahead Logging)** — To prevent data corruption during sudden crashes.

### Storage Improvements
- [ ] **VARCHAR / Variable-length types** — Beyond the current fixed-text approach.
- [ ] **Data Compression** — Page-level or row-level compression.
- [ ] **NULL bitmap optimization** — Reduce storage overhead for nullable columns.

### Network & Concurrency
- [ ] **Connection pooling & thread-safe executor** — Handle concurrent writes safely.
- [ ] **SSL/TLS support** — Encrypted connections.
- [ ] **Authentication & basic user management**.

---

## 🚀 Getting Started

### Prerequisites

1. **Install Go** (1.21+): [https://go.dev/dl/](https://go.dev/dl/)
2. **Install ncat** (for connecting to the server):

   - **Windows**: Download from [Nmap/ncat official site](https://nmap.org/download.html) or see [this guide on installing ncat on Windows and Linux](https://serverspace.io/support/help/how-to-install-ncat-tool-on_windows-and-linux/).
   - **Linux**: `sudo apt install ncat` (Debian/Ubuntu) or `sudo yum install nmap-ncat` (RHEL/CentOS).

### Run the Server

```bash
git clone https://github.com/Mohammad-y-abbass/moDB.git
cd moDB
go run main.go
```

The server starts on `localhost:3003`. You should see:
```
Server is running on port 3003
```

### Connect & Query

Open another terminal and use ncat:

```bash
ncat localhost 3003
```

Or on Windows, you can also use PowerShell:

```powershell
$client = New-Object System.Net.Sockets.TcpClient('localhost',3003);
$stream = $client.GetStream();
$reader = New-Object System.IO.StreamReader($stream);
$writer = New-Object System.IO.StreamWriter($stream);
$writer.AutoFlush = $true;
$reader.ReadLine(); # read prompt
$writer.WriteLine("SHOW DATABASES;");
$reader.ReadLine();
```

### Example Session

```
moDB> SHOW DATABASES;
+----------+
| Database |
+----------+
| test_db  |
+----------+

moDB> CREATE DATABASE mydb;
Success (Action completed)

moDB> USE mydb;
Success (Action completed)

moDB> SHOW TABLES;
+-------+
| Table |
+-------+
+-------+

moDB> CREATE TABLE users (id INT NOT NULL UNIQUE PRIMARY KEY, name TEXT NOT NULL, email TEXT);
Success (Action completed)

moDB> SHOW TABLES;
+-------+
| Table |
+-------+
| users |
+-------+

moDB> INSERT INTO users VALUES (1, Alice, alice@test.com);
Success (Action completed)

moDB> SELECT * FROM users;
+----+-------+-----------------+
| id | name  | email           |
+----+-------+-----------------+
| 1  | Alice | alice@test.com  |
+----+-------+-----------------+

moDB> DROP TABLE users;
Success (Action completed)

moDB> DROP DATABASE mydb;
Success (Action completed)
```

All queries must end with a semicolon (`;`) and are case-insensitive.

---

## 🧪 Running Tests

```bash
go test ./...
```

---

## 📁 Data Storage Layout

```
./data/
  └── <database_name>/
      ├── <table_name>.db      # Binary page data
      └── <table_name>.json    # Table schema / metadata
```

- Each database is a directory under `./data/`.
- Each table has a `.db` file (binary pages) and a `.json` file (schema definition).
- Pages are 4KB fixed-size, using a slotted-page architecture for row management.
