{{ config(
    materialized='table',
    alias='speed_deviation'
) }}

WITH speed_deviation_data AS (
    SELECT
        vp.track_id,
        ti.type,
        ABS(vp.speed - ti.avg_speed) AS speed_deviation
    FROM {{ source('traffic_data', 'vehicle_positions') }} vp
    JOIN {{ source('traffic_data', 'trajectory_info') }} ti ON vp.track_id = ti.track_id
)

SELECT
    type,
    AVG(speed_deviation) AS avg_speed_deviation
FROM speed_deviation_data
GROUP BY type
