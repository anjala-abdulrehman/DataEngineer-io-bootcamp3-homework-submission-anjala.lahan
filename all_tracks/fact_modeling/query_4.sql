/*
    User Devices Activity Int Datelist Implementation (query_4)
    Building on top of the previous question, convert the date list implementation into the base-2 integer
    datelist representation as shown in the fact data modeling day 2 lab.
    Assume that you have access to a table called user_devices_cumulated with the output of the above query.

 */

  WITH
  today AS (
    SELECT
      *
    FROM
      anjala.user_devices_cumulated
    WHERE
      DATE = DATE('2023-08-07')
  ),
  pow_of_two_dates_active AS (
    SELECT
      user_id,
      browser_type,
      SUM(
        CASE
          WHEN CONTAINS(dates_active, sequence_date) THEN POW(2, 30 - DATE_DIFF('day', sequence_date, DATE))
          ELSE 0
        END
      ) AS pow_2_is_active
    FROM
      today
      CROSS JOIN UNNEST (SEQUENCE(DATE('2023-08-01'), DATE('2023-08-07'))) AS t (sequence_date)
    GROUP BY
      user_id,
      browser_type
  )
SELECT
  user_id,
  browser_type,
  TO_BASE(CAST(pow_2_is_active AS INT), 2) AS user_active_days_binary
FROM
  pow_of_two_dates_active