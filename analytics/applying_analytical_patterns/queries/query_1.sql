-- Write a query (query_1) that does state change tracking for nba_players. Create a state change-tracking field that takes on the following values:
--
        -- A player entering the league should be New
        -- A player leaving the league should be Retired
        -- A player staying in the league should be Continued Playing
        -- A player that comes out of retirement should be Returned from Retirement
        -- A player that stays out of the league should be Stayed Retired

SELECT
    player_name,
    current_season,
    is_active,
    years_since_last_active,
    CASE
    WHEN years_since_last_active > 1 THEN 'Stayed Retired'
    WHEN years_since_last_active = 1 THEN 'Retired' --A player leaving the league should be Retired
    WHEN ROW_NUMBER() OVER (
        PARTITION BY
        player_name
        ORDER BY
        current_season
    ) = 1 THEN 'New' -- First season played for the player is 'New'
    WHEN (
        LAG(years_since_last_active, 1) OVER (
        PARTITION BY
            player_name
        ORDER BY
            current_season
        )
    ) = 1
    AND years_since_last_active = 0 THEN 'Returned from Retirement' -- if player was not playing in the last season but is playing now then the player has returned from retirement
    WHEN (
        LAG(years_since_last_active, 1) OVER (
        PARTITION BY
            player_name
        ORDER BY
            current_season
        )
    ) = 0
    AND years_since_last_active = 0 THEN 'Continued Playing'  -- if player was playing last season and this, then the player continued playing
    ELSE 'Stayed Retired'
    END as_state_indicator
FROM
    bootcamp.nba_players


-- Query Result

| player_name | current_season     | is_active     | years_since_last_active  | state_indicator  |
| -------------------------------------------------------------------------------------------------
| Brett Szabo | 	1996	   | true	   |            0	       | New             |
| Brett Szabo | 	1997	   | false	   |            1	       | Retired         |
| Brett Szabo | 	1998	   | false         |            2	       | Stayed Retired  |
| Brett Szabo | 	1998	   | false	   |            2	       | Stayed Retired  |
| Brett Szabo | 	1999	   | false	   |            3	       | Stayed Retired  |
