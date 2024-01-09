-- Actors History SCD Table Batch Backfill Query (query_4)
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO anjala.actors_history_scd
WITH
  actor_tbl_with_lag AS (
    SELECT
      actor,
      actor_id,
      films,
      current_year,
      LAG(current_year) OVER (PARTITION BY actor_id ORDER BY current_year) AS prev_year,
      LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
      is_active,
      quality_class
    FROM
      anjala.actors
    WHERE
      current_year < 2021
  ),
  actor_table_with_active_status AS (
    SELECT
      actor,
      actor_id,
      films,
      current_year,
      is_active,
      quality_class,
      is_active_last_year,
      prev_year,
      CASE
        WHEN prev_year IS NULL THEN 0 -- if this is the first year for the actor, mark did_change as '0', implying did not change
        WHEN current_year - prev_year = 1 AND is_active = is_active_last_year THEN 0 -- if previous year's data exist and is_active state matches then mark did_change as 0
        ELSE 1 -- ELSE condition stands for current_year - prev_year != 1 or is_active != is_active_last_year
      END AS did_change -- did active status change ?
    FROM
      actor_tbl_with_lag
  ),
  actor_table_with_active_streak AS (
    SELECT
      actor,
      actor_id,
      current_year,
      is_active,
      films,
      quality_class,
      CASE
        WHEN current_year - COALESCE(prev_year, current_year - 1) = 1 THEN COALESCE(prev_year, current_year - 1) + 1
        ELSE 0
      END AS streak
    FROM
      actor_table_with_active_status
  ),
  final_tbl AS (
    SELECT
      actor,
      actor_id,
      quality_class,
      ROW_NUMBER() OVER (
        PARTITION BY
          actor_id
        ORDER BY
          CASE quality_class
            WHEN 'star' THEN 1
            WHEN 'good' THEN 2
            WHEN 'average' THEN 3
            WHEN 'bad' THEN 4
          END
      ) AS rn, -- in the final tbl, the quality class would be the best quality class for the actor during that period
      is_active,
      MIN(current_year) OVER (
        PARTITION BY
          actor_id,
          streak
      ) AS start_date,
      MAX(current_year) OVER (
        PARTITION BY
          actor_id,
          streak
      ) AS end_date
    FROM
      actor_table_with_active_streak
  )
SELECT
  actor,
  actor_id,
  quality_class,
  is_active,
  start_date,
  end_date,
  end_date + 1 AS current_year
FROM
  final_tbl
WHERE
  rn = 1
ORDER BY
  actor_id,
  current_year
  -- filter for the best quality class