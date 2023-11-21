-- Actors History SCD Table Incremental Backfill Query (query_5)
-- Write an "incremental" query that can populate a single year's worth of the actors_history_scd table by combining the previous year's SCD 
-- data with the new incoming data from the actors table for this year
INSERT INTO anjala.actors_history_scd
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
      COALESCE(ty.current_year, ly.start_date) AS start_date,
      COALESCE(ty.current_year, ly.end_date) AS end_date,
      COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
      ly.is_active AS is_active_last_year,
      ty.is_active AS is_active_this_year,
      CASE
        WHEN ly.is_active <> ty.is_active THEN 1
        WHEN ly.is_active = ty.is_active THEN 0
      END AS did_change,
      2021 AS current_year
    FROM
      last_year ly
      FULL OUTER JOIN this_year ty ON ly.actor = ty.actor
      AND ly.end_date + 1 = ty.current_year
  ),
  changed AS (
    SELECT
      actor,
      quality_class,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          ROW (is_active_last_year, start_date, end_date + 1)
        ]
        WHEN did_change = 1 THEN ARRAY[ROW (is_active_this_year, start_date, end_date)]
        WHEN did_change IS NULL THEN ARRAY[
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
  quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date
FROM
  changed
  CROSS JOIN UNNEST (change_array) AS arr (is_active, start_date, end_date)
