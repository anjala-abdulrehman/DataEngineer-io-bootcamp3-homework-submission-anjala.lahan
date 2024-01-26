/*
User Devices Activity Datelist Implementation (query_3)
Write the incremental query to populate the table you wrote the DDL for in the above question
from the web_events and devices tables. This should look like the query to generate the cumulation
table from the fact modeling day 2 lab.
*/

INSERT INTO anjala.user_devices_cumulated
  WITH yesterday AS
(
  SELECT
  *
  FROM
  anjala.user_devices_cumulated
  WHERE date = DATE('2023-08-06') -- date changes to reflect day - 1 of the data load
),
today AS
(
SELECT
  e.user_id,
  d.browser_type,
  DATE(e.event_time) AS event_date
  FROM
  bootcamp.web_events e
  LEFT JOIN
  bootcamp.devices d
  ON
  d.device_id = e.device_id
  WHERE DATE(event_time) = DATE('2023-08-07') -- date changes to reflect day  of the data load
  GROUP BY e.user_id,
  d.browser_type,
  DATE(e.event_time)
)
SELECT
  COALESCE(t.user_id, y.user_id) as user_id,
  COALESCE(t.browser_type, y.browser_type) as browser_type,
  CASE
    WHEN y.dates_active IS NULL THEN ARRAY[t.event_date]
    WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
    END
    as event_date,
    DATE('2023-08-07') AS DATE -- date changes to reflect day  of the data load
  FROM
  today t
  FULL OUTER JOIN
  yesterday y
  ON t.user_id = y.user_id
  AND
  t.browser_type = y.browser_type