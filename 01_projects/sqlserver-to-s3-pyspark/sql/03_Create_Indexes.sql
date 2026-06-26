/*
===============================================================================
Project : SQL Server to Amazon S3 using PySpark
Script  : 03_Create_Indexes.sql

Purpose
-------
Create indexes AFTER loading approximately 10 million rows.

Why after?

Creating indexes after a bulk load is significantly faster because SQL Server
builds the index once instead of maintaining it for every inserted row.

Indexes created:

1. Load Date
   Used for validation and incremental ETL queries.

2. Batch ID
   Used to process or validate one ETL batch at a time.

3. Athlete ID
   Supports business-key lookups.

4. Hash Key
   Supports duplicate detection and change detection.

===============================================================================
*/

USE AdventureWorksDW2014;
GO

------------------------------------------------------------------------------
-- Index used for partition-style queries
------------------------------------------------------------------------------
CREATE INDEX IX_Athletes_Big_LoadDate
ON dbo.Athletes_Big
(
    load_year,
    load_month,
    load_day
);
GO

------------------------------------------------------------------------------
-- Index used for ETL batches
------------------------------------------------------------------------------
CREATE INDEX IX_Athletes_Big_BatchId
ON dbo.Athletes_Big
(
    batch_id
);
GO

------------------------------------------------------------------------------
-- Index used for business-key lookups
------------------------------------------------------------------------------
CREATE INDEX IX_Athletes_Big_AthleteId
ON dbo.Athletes_Big
(
    athlete_id
);
GO

------------------------------------------------------------------------------
-- Index used for hash-based searches
------------------------------------------------------------------------------
CREATE INDEX IX_Athletes_Big_HashKey
ON dbo.Athletes_Big
(
    athlete_hash_key
);
GO