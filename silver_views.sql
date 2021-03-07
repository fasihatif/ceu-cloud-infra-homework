-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `silver_views` TABLE
  CREATE TABLE fasihatif_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://fasih.atif/de4/views_silver'
    ) AS SELECT article,views,rank,date FROM fasihatif_homework.bronze_views
    
 