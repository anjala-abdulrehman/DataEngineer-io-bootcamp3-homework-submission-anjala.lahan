/*
User Devices Activity Datelist DDL (query_2)
The schema of this table should look like:
    user_id bigint
    browser_type varchar
    dates_active array(date)
    date date
*/


CREATE TABLE anjala.user_devices_cumulated
(
        user_id bigint,
        browser_type varchar,
        dates_active array(date),
        date date
        )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['date'] )