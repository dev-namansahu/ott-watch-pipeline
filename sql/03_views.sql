/* =====================================================================
   03_views.sql : reporting views for Power BI
   completion rate = completed sessions / sessions whose runtime is known
   ===================================================================== */
CREATE OR ALTER VIEW dwh.vw_top_shows_by_watch_hours AS
SELECT s.show_id, s.title, s.genre, s.content_type,
       COUNT(*)                                        AS sessions,
       COUNT(DISTINCT f.user_key)                      AS unique_viewers,
       CAST(SUM(f.watch_minutes) / 60.0 AS DECIMAL(12,2)) AS watch_hours,
       CAST(100.0 * SUM(CAST(f.completed_flag AS INT)) / NULLIF(COUNT(f.completed_flag), 0) AS DECIMAL(5,2)) AS completion_rate_pct,
       CAST(AVG(CAST(f.rating AS FLOAT)) AS DECIMAL(3,2)) AS avg_rating,
       RANK() OVER (ORDER BY SUM(f.watch_minutes) DESC) AS watch_rank
FROM dwh.fact_watch_events f JOIN dwh.dim_show s ON s.show_key = f.show_key
GROUP BY s.show_id, s.title, s.genre, s.content_type;
GO

CREATE OR ALTER VIEW dwh.vw_completion_by_genre AS
SELECT s.genre, COUNT(*) AS sessions,
       CAST(SUM(f.watch_minutes) / 60.0 AS DECIMAL(12,2)) AS watch_hours,
       CAST(100.0 * SUM(CAST(f.completed_flag AS INT)) / NULLIF(COUNT(f.completed_flag), 0) AS DECIMAL(5,2)) AS completion_rate_pct,
       CAST(AVG(f.completion_pct) AS DECIMAL(5,2)) AS avg_completion_pct
FROM dwh.fact_watch_events f JOIN dwh.dim_show s ON s.show_key = f.show_key
GROUP BY s.genre;
GO

/* daily device trend with a 7-day rolling total (window function) */
CREATE OR ALTER VIEW dwh.vw_device_trend AS
WITH d AS (
    SELECT dt.full_date, dv.device_type, COUNT(*) AS sessions, SUM(f.watch_minutes) / 60.0 AS watch_hours
    FROM dwh.fact_watch_events f
    JOIN dwh.dim_date dt ON dt.date_key = f.date_key
    JOIN dwh.dim_device dv ON dv.device_key = f.device_key
    GROUP BY dt.full_date, dv.device_type)
SELECT full_date, device_type, sessions, CAST(watch_hours AS DECIMAL(12,2)) AS watch_hours,
       CAST(SUM(watch_hours) OVER (PARTITION BY device_type ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS DECIMAL(12,2)) AS watch_hours_7d
FROM d;
GO

CREATE OR ALTER VIEW dwh.vw_user_engagement AS
SELECT u.user_id, u.country, u.[plan], COUNT(*) AS sessions, CAST(SUM(f.watch_minutes) / 60.0 AS DECIMAL(10,2)) AS watch_hours,
       COUNT(DISTINCT f.date_key) AS active_days
FROM dwh.fact_watch_events f JOIN dwh.dim_user u ON u.user_key = f.user_key
GROUP BY u.user_id, u.country, u.[plan];
GO

/* Example analytical query: top 3 shows per genre */
-- SELECT * FROM (SELECT genre, title, watch_hours, ROW_NUMBER() OVER (PARTITION BY genre ORDER BY watch_hours DESC) AS rn
--                FROM dwh.vw_top_shows_by_watch_hours) x WHERE rn <= 3;
