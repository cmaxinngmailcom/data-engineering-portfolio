USE AdventureWorksDW2014;
GO

SELECT COUNT(*) AS total_rows
FROM dbo.Athletes_Big;

SELECT
    batch_id,
    load_date,
    COUNT(*) AS row_count
FROM dbo.Athletes_Big
GROUP BY batch_id, load_date
ORDER BY batch_id;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT athlete_hash_key) AS distinct_hash_keys
FROM dbo.Athletes_Big;

SELECT
    MIN(athlete_sk) AS min_athlete_sk,
    MAX(athlete_sk) AS max_athlete_sk
FROM dbo.Athletes_Big;