WITH
  last_year AS ( -- last year data
    SELECT
      *
    FROM
      anjala.actors
    WHERE
      current_year = 2009
  ),
  this_year AS ( -- current year data
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(ROW (film, votes, rating, film_id)) AS films,
      CASE
        WHEN AVG(rating)  > 8 THEN 'star'
        WHEN AVG(rating) > 7 THEN 'good'
        WHEN AVG(rating) >  6 THEN 'average'
        ELSE 'bad'
      END  AS quality_class,
      True AS is_active, 
      YEAR AS current_year
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
      COALESCE(ty.actor, ly.actor) AS actor, -- if actor name for this year is null then get last year name
      COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
      COALESCE (ty.quality_class, ly.quality_class ) as quality_class,
      CASE
        WHEN ty.is_active AND ly.is_active THEN ly.films || ty.films
        WHEN ty.is_active AND NOT ly.is_active THEN ty.films
        WHEN NOT ty.is_active AND ly.is_active THEN ly.films
        WHEN ty.is_active AND ly.is_active IS NULL THEN ty.films
      END AS films,
      CASE
        WHEN ty.is_active is NULL then false
        WHEN ty.current_year IS NOT NULL then true
      END as is_active,
      ty.current_year
    FROM
      this_year ty
      FULL OUTER JOIN last_year ly ON ty.actor_id = ly.actor_id
  )
SELECT
  c.actor,
  c.actor_id,
  c.films,
  c.quality_class,
  is_active,
  c.current_year
FROM
  combined c
