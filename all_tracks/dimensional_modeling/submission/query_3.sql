-- Actors History SCD Table DDL (query_3)
-- Create a DDL for an actors_history_scd

CREATE TABLE anjala.actors_history_scd 
  ( actor varchar, 
  quality_class varchar, 
  is_active boolean, 
  start_date integer, 
  end_date integer ) 
WITH ( 
  format = 'PARQUET', 
  partitioning = ARRAY['start_date'] )
