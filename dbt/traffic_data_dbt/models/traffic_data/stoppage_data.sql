{{ config(
    materialized='table',
    alias='stoppage_data'
) }}

WITH stoppage_data AS (
    SELECT
        vp.track_id,
        ti.type,
        vp.time,
        vp.speed
    FROM {{ source('traffic_data', 'vehicle_positions') }} vp
    JOIN {{ source('traffic_data', 'trajectory_info') }} ti ON vp.track_id = ti.track_id
    WHERE vp.speed < 0.5
)

SELECT
    type,
    COUNT(DISTINCT track_id) AS num_stoppages
FROM stoppage_data
GROUP BY type
