def get_query(site_id, variant_code, interval, category_code):
    query = f"""
    DECLARE 
        @SiteId       UNIQUEIDENTIFIER = '{site_id}',
        @VariantCode  NVARCHAR(12)     = '{variant_code}',
        @Interval     NVARCHAR(50)     = '{interval}',
        @CategoryCode NVARCHAR(50)     = '{category_code}';

    WITH ProvisionedTimes AS (
        SELECT
            DATETRUNC(WEEK, MIN([From]))                      AS Starting,
            DATEADD(WEEK, 1, DATETRUNC(WEEK, MAX([To])))      AS Ending
        FROM [PollenSenseLive].[dbo].[AllProvisions] AP
        WHERE AP.SiteID = @SiteId
    ),
    provisionedHours AS (
        SELECT
            Starting,
            Ending,
            @SiteId       AS SiteId,
            @VariantCode  AS VariantCode,
            @Interval     AS [Interval],
            @CategoryCode AS CategoryCode
        FROM ProvisionedTimes

        UNION ALL

        SELECT
            CASE 
                WHEN @Interval = 'hour' THEN DATEADD(hour, 1, Starting)
                ELSE DATEADD(day,  1, Starting)
            END     AS Starting,
            Ending,
            SiteId,
            VariantCode,
            [Interval],
            CategoryCode
        FROM provisionedHours
        WHERE Starting < Ending
    ),
    FoundProvisionedHours AS (
        SELECT
            PH.SiteId,
            PH.VariantCode,
            PH.Starting,
            PH.CategoryCode,
            PH.[Interval],
            VR.[Count],
            VR.PPM3,
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM [PollenSenseLive].[dbo].[VariantRollup] VR2
                    WHERE VR2.Starting     = PH.Starting
                    AND VR2.SiteId       = PH.SiteId
                    AND VR2.VariantCode  = PH.VariantCode
                    AND VR2.[Interval]   = PH.[Interval]
                    
                ) THEN 1
                ELSE 0
            END AS IsRollup
        FROM provisionedHours PH
        LEFT JOIN [PollenSenseLive].[dbo].[VariantRollup] VR
        ON VR.Starting      = PH.Starting
        AND VR.SiteId        = PH.SiteId
        AND VR.VariantCode   = PH.VariantCode
        AND VR.[Interval]    = PH.[Interval]
        AND VR.CategoryCode  = PH.CategoryCode
    )
    SELECT
        SiteId,
        VariantCode,
        Starting,
        [Interval],
        CategoryCode,
        ISNULL([Count], 0) AS [Count],
        ISNULL(PPM3,     0) AS PPM3
    FROM FoundProvisionedHours FPH
    WHERE FPH.IsRollup = 1
    OPTION (MAXRECURSION 0);
"""
    return query


# Then run it as before:
# cursor.execute(query)
# rows = cursor.fetchall()
