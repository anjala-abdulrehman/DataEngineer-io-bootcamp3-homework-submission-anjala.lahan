-- Actors History SCD Table Incremental Backfill Query (query_5)
-- Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD
-- data with the new incoming data from the actors table for this year
WITH
  last_year AS (
    SELECT
      *
    FROM
      anjala.actors_history_scd
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      current_year,
      is_active
    FROM
      anjala.actors
    WHERE
      current_year = 2021
  ),
  combined AS (
    SELECT
      COALESCE(ty.actor, ly.actor) AS actor,
      COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
      COALESCE(ly.start_date, ty.current_year) AS start_date,
      COALESCE(ly.end_date, ty.current_year) AS end_date,
      ty.quality_class AS quality_class_this_year,
      ly.quality_class AS quality_class_last_year,
      ly.is_active AS is_active_last_year,
      ty.is_active AS is_active_this_year,
      CASE
        WHEN ly.is_active <> ty.is_active THEN 1
        WHEN ly.is_active = ty.is_active THEN 0
      END AS is_active_did_change
    FROM
      last_year ly
      FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
      AND ly.end_date + 1 = ty.current_year
  ),
  changed AS (
    SELECT
      actor,
      actor_id,
      CASE
        WHEN 'star' IN (quality_class_last_year, quality_class_this_year) THEN 'star'
        WHEN 'good' IN (quality_class_last_year, quality_class_this_year) THEN 'good'
        WHEN 'average' IN (quality_class_last_year, quality_class_this_year) THEN 'average'
        ELSE 'bad'
      END AS quality_class,
      CASE
        WHEN is_active_did_change = 0 THEN ARRAY[
          ROW (is_active_last_year, start_date, end_date + 1)
        ]
        WHEN is_active_did_change = 1 THEN ARRAY[ROW (is_active_this_year, start_date, end_date)]
        WHEN is_active_did_change IS NULL THEN ARRAY[
          ROW (
            COALESCE(is_active_this_year, is_active_last_year),
            start_date,
            end_date
          )
        ]
      END AS change_array
    FROM
      combined
  )
SELECT
  actor,
  actor_id,
  quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  arr.end_date + 1 AS current_year
FROM
  changed
  CROSS JOIN UNNEST (change_array) AS arr (is_active, start_date, end_date)
WHERE
  arr.end_date + 1 = 2022
  AND arr.end_date <> arr.start_date