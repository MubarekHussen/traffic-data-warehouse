{{ config(
    materialized='table',
    alias='trajectory_path_for_viz'
) }}

WITH ordered_trajectory AS (
    SELECT
        vp.track_id,
        vp.time,
        vp.lat,
        vp.lon,
        ti.type,
        ROW_NUMBER() OVER (PARTITION BY vp.track_id ORDER BY vp.time) AS point_sequence
    FROM {{ source('traffic_data', 'vehicle_positions') }} vp
    JOIN {{ source('traffic_data', 'trajectory_info') }} ti ON vp.track_id = ti.track_id
)

SELECT
    track_id,
    time,
    lat,
    lon,
    type,
    point_sequence
FROM ordered_trajectory