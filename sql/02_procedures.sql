/* =====================================================================
   02_procedures.sql : refresh warehouse after the Spark silver step (ADF Stored Procedure activity)
   ===================================================================== */
CREATE OR ALTER PROCEDURE dwh.usp_refresh_warehouse @run_date DATE, @pipeline_run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @staged BIGINT = (SELECT COUNT_BIG(*) FROM stg.watch_events), @loaded BIGINT, @orphans BIGINT;

    /* guard: staging must hold only rows for the date window being loaded */
    IF @staged = 0
    BEGIN
        INSERT ops.etl_run_log (pipeline_run_id, run_date, step, rows_staged, rows_loaded, status, message)
        VALUES (@pipeline_run_id, @run_date, 'refresh_warehouse', 0, 0, 'SKIPPED', 'no staged rows');
        RETURN;
    END;

    BEGIN TRAN;
        /* dimensions: upsert (SCD Type 1) */
        MERGE dwh.dim_show AS t USING stg.show AS s ON t.show_id = s.show_id
        WHEN MATCHED AND (t.title <> s.title OR t.genre <> s.genre OR t.content_type <> s.content_type
                          OR ISNULL(t.runtime_minutes,-1) <> ISNULL(s.runtime_minutes,-1) OR ISNULL(t.release_year,-1) <> ISNULL(s.release_year,-1))
            THEN UPDATE SET title = s.title, genre = s.genre, content_type = s.content_type, runtime_minutes = s.runtime_minutes,
                            release_year = s.release_year, updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (show_id, title, genre, content_type, release_year, runtime_minutes)
            VALUES (s.show_id, s.title, s.genre, s.content_type, s.release_year, s.runtime_minutes);

        MERGE dwh.dim_user AS t USING stg.[user] AS s ON t.user_id = s.user_id
        WHEN MATCHED AND (t.country <> s.country OR t.[plan] <> s.[plan] OR t.age_group <> s.age_group)
            THEN UPDATE SET country = s.country, [plan] = s.[plan], age_group = s.age_group, updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (user_id, country, [plan], age_group, signup_date)
            VALUES (s.user_id, s.country, s.[plan], s.age_group, s.signup_date);

        /* date dimension: add any missing days */
        INSERT dwh.dim_date (date_key, full_date, [year], [month], day_name, is_weekend)
        SELECT DISTINCT CONVERT(INT, FORMAT(e.event_date, 'yyyyMMdd')), e.event_date, YEAR(e.event_date), MONTH(e.event_date),
               DATENAME(WEEKDAY, e.event_date), CASE WHEN DATEPART(WEEKDAY, e.event_date) IN (1, 7) THEN 1 ELSE 0 END
        FROM stg.watch_events e
        WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_date d WHERE d.full_date = e.event_date);

        /* fact: idempotent - delete the dates being reloaded, then insert */
        DELETE f FROM dwh.fact_watch_events f
        WHERE f.date_key IN (SELECT DISTINCT CONVERT(INT, FORMAT(event_date, 'yyyyMMdd')) FROM stg.watch_events);

        INSERT dwh.fact_watch_events (session_id, date_key, user_key, show_key, device_key, event_ts, watch_minutes, rating,
                                      completion_pct, completed_flag, over_100_flag, runtime_missing_flag)
        SELECT e.session_id, CONVERT(INT, FORMAT(e.event_date, 'yyyyMMdd')), u.user_key, sh.show_key, e.device_key, e.event_ts,
               e.watch_minutes, e.rating, e.completion_pct, e.completed_flag, e.over_100_flag, e.runtime_missing_flag
        FROM stg.watch_events e
        JOIN dwh.dim_user u  ON u.user_id  = e.user_id
        JOIN dwh.dim_show sh ON sh.show_id = e.show_id;
        SET @loaded = @@ROWCOUNT;
        SET @orphans = @staged - @loaded;       -- rows lost to a missing dimension key must be investigated
    COMMIT;

    INSERT ops.etl_run_log (pipeline_run_id, run_date, step, rows_staged, rows_loaded, rows_rejected, status, message)
    VALUES (@pipeline_run_id, @run_date, 'refresh_warehouse', @staged, @loaded, @orphans,
            CASE WHEN @orphans = 0 THEN 'OK' ELSE 'WARN' END,
            CASE WHEN @orphans = 0 THEN NULL ELSE 'staged rows without matching dimension key' END);
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_log_failure @run_date DATE, @pipeline_run_id VARCHAR(64), @step VARCHAR(40), @message VARCHAR(400)
AS
    INSERT ops.etl_run_log (pipeline_run_id, run_date, step, status, message) VALUES (@pipeline_run_id, @run_date, @step, 'FAILED', @message);
GO
