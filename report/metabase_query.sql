SELECT
    drive_id,
    trip_id,
    frames,
    frames_kept,
    frames - frames_kept AS frames_dropped,
    ROUND(
        100.0 * (frames - frames_kept)
        / NULLIF(frames, 0),
        2
    ) AS dropped_pct,
    avg_speed_kmh
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, trip_id
LIMIT 5;
