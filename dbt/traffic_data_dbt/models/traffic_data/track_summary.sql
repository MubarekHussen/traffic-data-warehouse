{{ config(
    materialized='table',
    alias='track_summary'
) }}

WITH track_summary AS (
    SELECT
        type,
        SUM(traveled_d) AS total_distance,
        AVG(avg_speed) AS average_speed
    FROM {{ source('traffic_data', 'trajectory_info') }}
    GROUP BY type
)

SELECT * FROM track_summary