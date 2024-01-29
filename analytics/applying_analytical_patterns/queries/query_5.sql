-- Build additional queries on top of the results of the GROUPING SETS aggregations
-- above to answer the following questions:
--
-- Write a query (query_5) to answer: "Which team has won the most games"

WITH
  nba_game_details AS (
    SELECT
      d.game_id AS dim_game_id,
      d.team_id AS dim_team_id,
      d.player_id AS dim_player_id,
      d.team_abbreviation AS fct_team_abbr,
      d.team_city AS fct_team_city,
      d.player_name AS fct_player_name,
      d.pts,
      CASE
        WHEN home_team_wins = 1 THEN home_team_id
        ELSE visitor_team_id
      END AS winner,
      YEAR(g.game_date_est) AS season
    FROM
      bootcamp.nba_game_details d
      LEFT JOIN bootcamp.nba_games g ON d.game_id = g.game_id
  ),
  STATS AS (
    SELECT
      fct_player_name,
      fct_team_abbr,
      season,
      SUM(pts) AS ttl_points_scored,
      COUNT(winner) AS ttl_wins
    FROM
      nba_game_details
    GROUP BY
      GROUPING SETS (
        (fct_player_name, fct_team_abbr),
        (fct_player_name, season),
        (fct_team_abbr)
      )
  )
SELECT
  fct_player_name,
  season,
  ttl_points_scored,
  RANK() OVER (
    ORDER BY
      ttl_points_scored DESC
  ) AS RANK
FROM
  STATS
WHERE
  season IS NOT NULL
  AND fct_player_name IS NOT NULL


| fct_team_abbr | ttl_wins | RANK |
|---------------|----------|------|
| MIA           | 24032    | 1    |
| BOS           | 23851    | 2    |
| SAS           | 23621    | 3    |
| LAL           | 23259    | 4    |
| DAL           | 23099    | 5    |
