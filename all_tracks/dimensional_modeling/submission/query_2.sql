-- Cumulative Table Computation Query (query_2)
-- Write a query that populates the actors table one year at a time.
-- Below query loads data for the year 2010, increament current_year and YEAR by 1 to add data for subsequent years

INSERT INTO
  anjala.actors
WITH
  last_year AS (
    SELECT
      *
    FROM
      anjala.actors
    WHERE
      current_year = 2009
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(ROW (film, votes, rating, film_id)) AS films,
      YEAR AS current_year,
      YEAR IS NOT NULL AS is_active
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 2010
    GROUP BY
      actor,
      actor_id,
      YEAR
  ),
  combined AS (
    SELECT
      COALESCE(ty.actor, ly.actor) AS actor,
      COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
      CASE
        WHEN ty.is_active
        AND ly.is_active THEN ly.films || ty.films
        WHEN ty.is_active
        AND NOT ly.is_active THEN ty.films
        WHEN NOT ty.is_active
        AND ly.is_active THEN ly.films
        WHEN ty.is_active
        AND ly.is_active IS NULL THEN ty.films
      END AS films,
      ty.current_year IS NOT NULL AS is_active,
      ty.current_year
    FROM
      this_year ty
      FULL OUTER JOIN last_year ly ON ty.actor = ly.actor
  )
SELECT
  c.actor,
  c.actor_id,
  c.films,
  CASE
    WHEN AVG(arr.rating) > 8 THEN 'star'
    WHEN AVG(arr.rating) > 7
    AND AVG(arr.rating) <= 8 THEN 'good'
    WHEN AVG(arr.rating) > 6
    AND AVG(arr.rating) <= 7 THEN 'average'
    WHEN AVG(arr.rating) <= 6 THEN 'bad'
  END AS quality_class,
  MAX(c.is_active) AS is_active,
  c.current_year
FROM
  combined c
  CROSS JOIN UNNEST (films) AS arr (film, votes, rating, film_id)
GROUP BY
  c.actor,
  c.actor_id,
  c.films,
  c.current_year
