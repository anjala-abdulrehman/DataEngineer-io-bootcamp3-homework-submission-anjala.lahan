/*
Write a query (query_6) that uses window functions on nba_game_details to answer the question: "What is the most games a single team has won in a given 90-game stretch?"
*/
WITH
  points_tbl AS (
    SELECT DISTINCT
      game_id,
      team_id,
      team_abbreviation,
      SUM(pts) OVER (
        PARTITION BY
          game_id,
          team_id
      ) AS ttl_pts
    FROM
      bootcamp.nba_game_details
    WHERE CAST(SPLIT(min, ':')[1] AS DOUBLE) < 90
  ),
  game_outcome_tbl AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY
          game_id
        ORDER BY
          ttl_pts DESC
      ) AS game_outcome
    FROM
      points_tbl
  ),
  stats_tbl AS (
    SELECT
      team_abbreviation,
      COUNT(
        CASE
          WHEN game_outcome = 1 THEN 1
        END
      ) AS num_wins,
      COUNT(
        CASE
          WHEN game_outcome = 2 THEN 1
        END
      ) AS num_loses
    FROM
      game_outcome_tbl
    GROUP BY
      team_abbreviation
  )
SELECT
  team_abbreviation AS team_with_the_most_wins
FROM
  stats_tbl
WHERE
  num_wins = (
    SELECT
      MAX(num_wins)
    FROM
      stats_tbl
  )

| team_with_the_most_wins |	num_wins |
--------------------------------------
|           SAS	          | 1184     |
