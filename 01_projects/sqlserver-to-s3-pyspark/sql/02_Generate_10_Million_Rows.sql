/*
===============================================================================
Project : SQL Server → Amazon S3 using PySpark
Script  : 02_Generate_10_Million_Rows.sql

Author  : Claude Maxime Innocent

Purpose
-------
Populate dbo.Athletes_Big with approximately 10 million rows.

Strategy
--------
Current Athletes table:

≈145,054 rows

The script duplicates the data across 70 fictional ETL batches.

145,054 × 70

≈10,153,780 rows

Each batch receives:

• Different hash key
• Different load timestamp
• Different fictional load date
• Different batch id

This simulates 70 daily ETL loads.

Benefits
--------
✓ Large enough for Spark testing

✓ Large enough for JDBC partitioning

✓ Realistic ETL metadata

✓ Future S3 partitioning

===============================================================================
*/

USE AdventureWorksDW2014;
GO

------------------------------------------------------------------------------
-- Create 70 fictional ETL batches.
------------------------------------------------------------------------------
;WITH Numbers AS
(
    SELECT TOP (70)

        ROW_NUMBER() OVER
        (
            ORDER BY (SELECT NULL)
        ) AS batch_id

    FROM sys.all_objects a

    CROSS JOIN sys.all_objects b
)

------------------------------------------------------------------------------
-- Insert approximately 10 million rows.
------------------------------------------------------------------------------
INSERT INTO dbo.Athletes_Big
(
    athlete_hash_key,

    athlete_id,
    name,
    born_date,
    born_city,
    born_region,
    born_country,
    NOC,
    height_cm,
    weight_kg,
    died_date,

    batch_id,
    source_system,

    load_timestamp,
    load_date,
    load_year,
    load_month,
    load_day
)

SELECT

    --------------------------------------------------------------------------
    -- SHA-256 Hash Key
    --------------------------------------------------------------------------
    CONVERT
    (
        CHAR(64),

        HASHBYTES
        (
            'SHA2_256',

            CONCAT
            (
                n.batch_id,
                '|',

                a.athlete_id,
                '|',

                ISNULL(a.name,''),

                '|',

                ISNULL(CONVERT(VARCHAR(10),a.born_date,120),''),

                '|',

                ISNULL(a.born_country,'')
            )

        ),

        2

    ) AS athlete_hash_key,

    --------------------------------------------------------------------------
    -- Source columns
    --------------------------------------------------------------------------
    a.athlete_id,
    a.name,
    a.born_date,
    a.born_city,
    a.born_region,
    a.born_country,
    a.NOC,
    a.height_cm,
    a.weight_kg,
    a.died_date,

    --------------------------------------------------------------------------
    -- Metadata
    --------------------------------------------------------------------------
    n.batch_id,

    'SQLSERVER' AS source_system,

    --------------------------------------------------------------------------
    -- Simulated ETL execution timestamp
    --------------------------------------------------------------------------
    DATEADD
    (
        SECOND,
        n.batch_id,

        CAST
        (
            DATEADD
            (
                DAY,
                n.batch_id-1,
                '2024-01-01'
            )

            AS DATETIME2
        )

    ) AS load_timestamp,

    --------------------------------------------------------------------------
    -- Fictional ETL load date
    --------------------------------------------------------------------------
    DATEADD
    (
        DAY,
        n.batch_id-1,
        '2024-01-01'
    ) AS load_date,

    YEAR
    (
        DATEADD(DAY,n.batch_id-1,'2024-01-01')
    ) AS load_year,

    MONTH
    (
        DATEADD(DAY,n.batch_id-1,'2024-01-01')
    ) AS load_month,

    DAY
    (
        DATEADD(DAY,n.batch_id-1,'2024-01-01')
    ) AS load_day

FROM dbo.Athletes a

------------------------------------------------------------------------------
-- Duplicate every athlete for every fictional ETL batch.
------------------------------------------------------------------------------
CROSS JOIN Numbers n;
GO