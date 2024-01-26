/* Write a query to de-duplicate the nba_game_details table from the day 1 lab of the fact modeling week 2 so there are no duplicate values.
 You should de-dupe based on the combination of game_id, team_id and player_id, since a player cannot have more than 1 entry per game.
 Feel free to take the first value here.
*/
WITH
  duplicated_data AS (
    SELECT
      *,
      -- generating row number based on unique_combination, and later pick the first one
      ROW_NUMBER() OVER (
        PARTITION BY
          game_id,
          team_id,
          player_id
      ) AS seq_number
    FROM
      bootcamp.nba_game_details
  )
SELECT
  game_id,
  team_id,
  team_abbreviation,
  team_city,
  player_id,
  player_name,
  nickname,
  start_position,
  COMMENT,
  MIN,
  fgm,
  fga,
  fg_pct,
  fg3m,
  fg3a,
  fg3_pct,
  ftm,
  fta,
  ft_pct,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  TO,
  pf,
  pts,
  plus_minus
FROM
  duplicated_data
WHERE
  seq_number = 1