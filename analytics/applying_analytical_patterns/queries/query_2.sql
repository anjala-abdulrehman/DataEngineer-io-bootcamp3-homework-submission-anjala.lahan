-- Write a query (query_2) that uses GROUPING SETS to perform aggregations of the nba_game_details data. Create slices that aggregate along the following combinations of dimensions:
--
    -- player and team
    -- player and season
    -- team

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
      LEFT JOIN bootcamp.nba_games g ON d.game_id = g.game_id -- required cols for
  )
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


| fct_player_name            | fct_team_abbr | season | ttl_points_scored | ttl_wins |
|----------------------------|---------------|--------|-------------------|----------|
| Keita Bates-Diop           | SAS           | null   | 671               | 159      |
| Pat Connaughton            | MIL           | null   | 2538              | 391      |
| Thanasis Antetokounmpo     | MIL           | null   | 494               | 276      |
| Ayo Dosunmu                | CHI           | null   | 1040              | 120      |
| Jalen Johnson              | ATL           | null   | 228               | 93       |
