{{ config(
    materialized='table',
    alias='speed_over_time'
) }}

WITH speed_time_data AS (
    SELECT
        vp.track_id,
        ti.type,
        vp.time,
        vp.speed
    FROM {{ source('traffic_data', 'vehicle_positions') }} vp
    JOIN {{ source('traffic_data', 'trajectory_info') }} ti ON vp.track_id = ti.track_id
)

SELECT
    type,
    time,
    AVG(speed) AS avg_speed
FROM speed_time_data
GROUP BY type, time
