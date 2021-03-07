-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
  CREATE TABLE fasihatif_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://fasih.atif/de4/gold_allviews'
    ) AS SELECT DISTINCT(article),
                SUM(views) AS total_top_view,
                MIN(rank) AS top_rank,
                COUNT(article) AS ranked_days 
                FROM fasihatif_homework.silver_views
                GROUP BY article
                ORDER BY total_top_view DESC;
