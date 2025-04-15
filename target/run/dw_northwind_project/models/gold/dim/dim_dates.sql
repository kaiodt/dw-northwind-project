
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."dim_dates__dbt_tmp__dbt_tmp_vw" as 

-- Set the start and end dates for the date dimension



WITH numbers AS (
  SELECT TOP (SELECT DATEDIFF(DAY, ''1990-01-01'', ''2050-12-31'') + 1)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
  
  FROM
    (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS e(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS f(n)
    CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS g(n)
),

calendar AS (
  SELECT
    DATEADD(DAY, n, CAST(''1990-01-01'' AS DATE)) AS date_day
  
  FROM
    numbers
),

dates AS (
  SELECT
    CAST(FORMAT(date_day, ''yyyyMMdd'') AS INT) AS date_sk,
    date_day,
    DAY(date_day) AS day,
    MONTH(date_day) AS month,
    DATENAME(month, date_day) AS month_name,
    DATEPART(QUARTER, date_day) AS quarter,
    ''Q'' + CAST(DATEPART(QUARTER, date_day) AS VARCHAR) AS quarter_name,
    YEAR(date_day) AS year,
    DATEPART(WEEK, date_day) AS week,
    DATEPART(WEEKDAY, date_day) AS day_of_week,
    DATENAME(WEEKDAY, date_day) AS day_name,
    FORMAT(date_day, ''yyyy'') + ''-W'' +
      RIGHT(''0'' + CAST(DATEPART(WEEK, date_day) AS VARCHAR), 2) AS year_week,
    FORMAT(date_day, ''yyyy-MM'') AS year_month,
    FORMAT(date_day, ''yyyy'') + ''-Q'' +
      CAST(DATEPART(QUARTER, date_day) AS VARCHAR) AS year_quarter,
    CASE
      WHEN DATEPART(WEEKDAY, date_day) IN (1, 7) THEN 1
      ELSE 0
    END AS is_weekend
  
  FROM
    calendar
  
  WHERE
    date_day <= ''2050-12-31''
)

SELECT * FROM dates;;
    ')

EXEC('
            SELECT * INTO "DW_GOLD"."dbo"."dim_dates__dbt_tmp" FROM "DW_GOLD"."dbo"."dim_dates__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.dim_dates__dbt_tmp__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_dim_dates__dbt_tmp_cci'
        AND object_id=object_id('dbo_dim_dates__dbt_tmp')
    )
    DROP index "dbo"."dim_dates__dbt_tmp".dbo_dim_dates__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_dim_dates__dbt_tmp_cci
    ON "dbo"."dim_dates__dbt_tmp"

   


  