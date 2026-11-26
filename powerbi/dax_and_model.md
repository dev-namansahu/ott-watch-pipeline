# Power BI semantic model

## Load
Option A (local demo): Get Data → Text/CSV → `data/lake/gold_csv/` : `fact_watch_events`, `dim_show`, `dim_user`, `dim_device`, `dim_date`.
Option B (Azure): connect to Azure SQL `dwh.fact_watch_events`, `dwh.dim_*` (Import mode, scheduled refresh after the ADF run).

## Relationships (star schema, single-direction, many-to-one from fact)
`fact_watch_events[show_id] → dim_show[show_id]` · `[user_id] → dim_user[user_id]` · `[device_key] → dim_device[device_key]` · `[event_date] → dim_date[date]`
(Azure version: use the surrogate `*_key` columns instead.) Mark `dim_date` as the date table. No bi-directional filters.

## DAX measures
```DAX
Sessions            = COUNTROWS ( fact_watch_events )
Watch Hours         = DIVIDE ( SUM ( fact_watch_events[watch_minutes] ), 60 )
Unique Viewers      = DISTINCTCOUNT ( fact_watch_events[user_id] )
Avg Watch Min / Session = DIVIDE ( SUM ( fact_watch_events[watch_minutes] ), [Sessions] )

-- completion rate only counts sessions whose runtime is known (completed_flag is blank otherwise)
Completed Sessions  = CALCULATE ( COUNTROWS ( fact_watch_events ), fact_watch_events[completed_flag] = TRUE () )
Sessions w/ Runtime = CALCULATE ( COUNTROWS ( fact_watch_events ), NOT ISBLANK ( fact_watch_events[completed_flag] ) )
Completion Rate %   = DIVIDE ( [Completed Sessions], [Sessions w/ Runtime] )
Avg Completion %    = AVERAGE ( fact_watch_events[completion_pct] ) / 100
Avg Rating          = AVERAGE ( fact_watch_events[rating] )

Device Share %      = DIVIDE ( [Watch Hours], CALCULATE ( [Watch Hours], ALLSELECTED ( dim_device ) ) )
Watch Hours 7d Avg  = CALCULATE ( DIVIDE ( [Watch Hours], 7 ), DATESINPERIOD ( dim_date[date], MAX ( dim_date[date] ), -7, DAY ) )
Watch Hours vs Prev Day % = VAR prev = CALCULATE ( [Watch Hours], DATEADD ( dim_date[date], -1, DAY ) ) RETURN DIVIDE ( [Watch Hours] - prev, prev )
Weekend Watch Share % = DIVIDE ( CALCULATE ( [Watch Hours], dim_date[is_weekend] = TRUE () ), [Watch Hours] )
```

## Report pages
1. **Overview** – cards: Watch Hours, Sessions, Unique Viewers, Completion Rate %, Avg Rating; daily Watch Hours line with 7-day average.
2. **Content** – top 10 shows by Watch Hours (bar), Completion Rate % by genre (bar), scatter of Watch Hours vs Completion Rate % per show.
3. **Devices & trends** – Watch Hours by device over time (stacked area), Device Share % (donut), weekday vs weekend split by device (matrix on `dim_date[is_weekend]`).
4. **Audience** – Watch Hours by country and plan, sessions by age group.
Slicers: date range, genre, content type, device.

## Data-quality note for the report footer
Sessions whose show has no runtime (about 3% in the demo data) have no completion % and are excluded from Completion Rate %.
