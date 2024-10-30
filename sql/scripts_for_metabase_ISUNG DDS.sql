-- Доля учебных планов в статусе "verified"
SELECT 
    CAST(SUM(CASE WHEN (on_check = 'verified') THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) AS ratio
FROM dds.up
WHERE year = 2023;

-- Доля учебных планов с некорректной трудоёмкостью 
SELECT 
    CAST(SUM(CASE WHEN (laboriousness = 240 AND qualification = 'bachelor') OR (laboriousness = 120 AND qualification = 'master') THEN 0 ELSE 1 END) AS FLOAT) / COUNT(*) AS ratio
FROM dds.up
WHERE year = 2023;

-- Доля предметов в статусе "одобрено" в динамике
WITH LastRows AS (
    SELECT DISTINCT ON (wp_id) *
    FROM dds.wp, dds.states st
    where wp.wp_status=st.id
    ORDER BY wp_id, update_ts DESC
),
RankedData AS (
    SELECT
        *,
        CASE 
            WHEN {{time_interval}} = 'Минута' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI')
            WHEN {{time_interval}} = 'Час' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24')
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
        END AS time_interval,
        COUNT(*) OVER (ORDER BY update_ts) AS running_total,
        SUM(CASE WHEN lr.cop_state = 'AC' THEN 1 ELSE 0 END) OVER (ORDER BY update_ts) AS ac_count
    FROM LastRows lr
)
SELECT
    time_interval,
    MAX(running_total) AS total_count_disc,
    MAX(ac_count) AS count_ac_status,
    MAX(ac_count) / MAX(running_total)::FLOAT AS ac_status_share
FROM RankedData
GROUP BY time_interval
ORDER BY time_interval;


-- Доля заполненных аннотаций в динамике
WITH LastRows AS (
    SELECT DISTINCT ON (wp_id) *
    FROM dds.wp
    ORDER BY wp_id, update_ts DESC
),
RankedData AS (
    SELECT
        *,
        CASE 
            WHEN {{time_interval}} = 'Минута' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI')
            WHEN {{time_interval}} = 'Час' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24')
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
        END AS time_interval,
        COUNT(*) OVER (ORDER BY update_ts) AS running_total,
        SUM(CASE WHEN wp_description IS NOT NULL AND wp_description <> '' THEN 1 ELSE 0 END) OVER (ORDER BY update_ts) AS running_completed_annotations
    FROM LastRows
)
SELECT
    time_interval,
    MAX(running_total) AS total_count_annotations,
    MAX(running_completed_annotations) AS count_completed_annotations,
    MAX(running_completed_annotations) / MAX(running_total)::FLOAT AS annotation_share
FROM RankedData
GROUP BY time_interval
ORDER BY time_interval;