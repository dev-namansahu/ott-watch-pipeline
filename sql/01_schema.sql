/* =====================================================================
   OTT Watch-History Warehouse - Azure SQL / SQL Server (T-SQL)
   01_schema.sql : staging, star schema, run log
   ===================================================================== */
IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
IF SCHEMA_ID('dwh') IS NULL EXEC('CREATE SCHEMA dwh');
IF SCHEMA_ID('ops') IS NULL EXEC('CREATE SCHEMA ops');
GO

/* ---------- staging (ADF Copy activity truncates and loads these) ---------- */
CREATE TABLE stg.show   (show_id VARCHAR(10), title VARCHAR(200), genre VARCHAR(30), release_year INT, runtime_minutes INT, content_type VARCHAR(10));
CREATE TABLE stg.[user] (user_id VARCHAR(10), country VARCHAR(40), [plan] VARCHAR(15), age_group VARCHAR(10), signup_date DATE);
CREATE TABLE stg.watch_events (
    event_date DATE, session_id VARCHAR(20), user_id VARCHAR(10), show_id VARCHAR(10), device_key INT, device_type VARCHAR(10),
    event_ts DATETIME2(0), watch_minutes INT, rating TINYINT NULL, completion_pct DECIMAL(5,2) NULL,
    completed_flag BIT NULL, over_100_flag BIT NULL, runtime_missing_flag BIT);
GO

/* ---------- dimensions ---------- */
CREATE TABLE dwh.dim_device (device_key INT PRIMARY KEY, device_type VARCHAR(10) NOT NULL UNIQUE);
INSERT dwh.dim_device VALUES (1,'MOBILE'),(2,'TV'),(3,'WEB'),(4,'TABLET'),(5,'UNKNOWN');

CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,            -- yyyymmdd
    full_date DATE NOT NULL UNIQUE, [year] SMALLINT, [month] TINYINT, day_name VARCHAR(10), is_weekend BIT);

CREATE TABLE dwh.dim_show (
    show_key INT IDENTITY(1,1) PRIMARY KEY, show_id VARCHAR(10) NOT NULL UNIQUE, title VARCHAR(200) NOT NULL,
    genre VARCHAR(30) NOT NULL, content_type VARCHAR(10) NOT NULL, release_year INT NULL, runtime_minutes INT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());

CREATE TABLE dwh.dim_user (
    user_key INT IDENTITY(1,1) PRIMARY KEY, user_id VARCHAR(10) NOT NULL UNIQUE, country VARCHAR(40) NOT NULL,
    [plan] VARCHAR(15) NOT NULL, age_group VARCHAR(10) NOT NULL, signup_date DATE NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());

/* ---------- fact: one row per unique watch session ---------- */
CREATE TABLE dwh.fact_watch_events (
    session_id VARCHAR(20) NOT NULL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    user_key INT NOT NULL REFERENCES dwh.dim_user(user_key),
    show_key INT NOT NULL REFERENCES dwh.dim_show(show_key),
    device_key INT NOT NULL REFERENCES dwh.dim_device(device_key),
    event_ts DATETIME2(0) NOT NULL,
    watch_minutes INT NOT NULL CHECK (watch_minutes >= 0),
    rating TINYINT NULL CHECK (rating BETWEEN 1 AND 5),
    completion_pct DECIMAL(5,2) NULL CHECK (completion_pct BETWEEN 0 AND 100),
    completed_flag BIT NULL, over_100_flag BIT NULL, runtime_missing_flag BIT NOT NULL,
    loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
CREATE INDEX ix_fact_date   ON dwh.fact_watch_events(date_key) INCLUDE (show_key, device_key, watch_minutes, completed_flag);
CREATE INDEX ix_fact_show   ON dwh.fact_watch_events(show_key)  INCLUDE (watch_minutes, completed_flag, rating);
GO

CREATE TABLE ops.etl_run_log (
    run_id BIGINT IDENTITY(1,1) PRIMARY KEY, pipeline_run_id VARCHAR(64), run_date DATE NOT NULL, step VARCHAR(40) NOT NULL,
    rows_staged BIGINT NULL, rows_loaded BIGINT NULL, rows_rejected BIGINT NULL, status VARCHAR(10) NOT NULL,
    message VARCHAR(400) NULL, logged_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
GO
