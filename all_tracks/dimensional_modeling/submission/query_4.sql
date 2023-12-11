-- Actors History SCD Table Batch Backfill Query (query_4)
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO anjala.actors_history_scd 
with actor_tbl_with_lag AS
(
 SELECT 
  actor,
  films,
  current_year,
  is_active,
  LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as is_active_last_year
  FROM
  anjala.actors
  WHERE current_year < 2021
),
actor_table_with_active_status AS
(
SELECT
  actor,
  films,
  current_year,
  is_active,
  CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0
  END as did_change
  FROM
   actor_tbl_with_lag
),
actor_table_with_active_streak AS
(
SELECT
  actor,
  current_year,
  is_active,
  films,
  SUM(did_change) OVER (PARTITION BY actor, current_year) as streak
  FROM actor_table_with_active_status
)
SELECT
  actor,
  CASE
    WHEN AVG(arr.rating) > 8 THEN 'star'
    WHEN AVG(arr.rating) > 7
    AND AVG(arr.rating) <= 8 THEN 'good'
    WHEN AVG(arr.rating) > 6
    AND AVG(arr.rating) <= 7 THEN 'average'
    WHEN AVG(arr.rating) <= 6 THEN 'bad'
  END AS quality_class,
  MAX(is_active) as is_active,
  MIN(current_year) as start_date,
  MAX(current_year) as end_date  
FROM
  actor_table_with_active_streak
  CROSS JOIN UNNEST (films) AS arr(film, votes, rating, film_id)
GROUP BY actor, streak
