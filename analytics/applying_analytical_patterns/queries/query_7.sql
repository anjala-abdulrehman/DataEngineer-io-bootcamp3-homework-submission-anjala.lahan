
-- Write a query (query_7) that uses window functions on nba_game_details to answer the question:
-- "How many games in a row did LeBron James score over 10 points a game?"

/*
 In this query, any row of data will give the number of times LeBron James consecutively scored over 10 points
 to get the max of times, he has done this in his career, check for MAX(num_consecutive_10_or_more_score)
 */
WITH
  nba_game_details AS (
    SELECT
      d.player_name,
      g.game_date_est,
      d.pts,
      CASE
        WHEN pts > 10 THEN TRUE
        ELSE FALSE
      END AS points_exceed_10_in_curr_game
    FROM
      bootcamp.nba_game_details d
      INNER JOIN bootcamp.nba_games g ON d.game_id = g.game_id -- join table to get all necessary details for the query
    WHERE
      player_name = 'LeBron James'
  ),
  concecutive_games AS (
    SELECT
      game_date_est,
      player_name,
      pts,
      CASE
        WHEN (
          points_exceed_10_in_curr_game
          AND (
            LAG(points_exceed_10_in_curr_game, 1) OVER (
              ORDER BY
                game_date_est
            )
          )
        ) THEN 1
        ELSE 0
      END has_streak -- has player score more than 10 in the last game <LOGICAL> AND this game
    FROM
      nba_game_details
  )
SELECT
  *,
  SUM(has_streak) OVER (
    ORDER BY
      game_date_est
  ) as num_consecutive_10_or_more_score
FROM
  concecutive_games

| num_consecutive_10_or_more_score | 
| 1624                             |
