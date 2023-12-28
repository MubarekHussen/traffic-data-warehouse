{{ config(
    materialized='table',
    alias='trajectory_path'
) }}

WITH trajectory_path AS (
    SELECT
        vp.track_id,
        vp.time,
        vp.lat,
        vp.lon,
        ti.type
    FROM {{ source('traffic_data', 'vehicle_positions') }} vp
    JOIN {{ source('traffic_data', 'trajectory_info') }} ti ON vp.track_id = ti.track_id
)

SELECT *
FROM trajectory_path
