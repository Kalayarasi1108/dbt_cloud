-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_SUM(SAT_UAC_SECONDARY_SCHOOL_SUM_SK, HUB_UAC_SECONDARY_SCHOOL_KEY,
                                                         SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5, YEAR, CODE, NAME,
                                                         VETFLAG, SCHADDR1, SCHADDR2, SCHADDR3, SCHADDR4, SCHPCODE,
                                                         SCHEMAIL, CNTRY, OFFSHORE, STATE, DISADV, SCHLANG, SCHABSRA,
                                                         RURAL, ICSEA, LATITUDE, LONGITUDE, MESH_BLOCK,
                                                         DISTANCE_FROM_CAMPUS, IS_DELETED)

WITH SAT_SEC_SCHOOL AS (
    SELECT HUB_UAC_SECONDARY_SCHOOL_KEY,
           SOURCE,
           HASH_MD5,
           LOAD_DTS,
           LEAD(LOAD_DTS)
                OVER (PARTITION BY HUB_UAC_SECONDARY_SCHOOL_KEY,HASH_MD5 ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_SUM
)

   , UNIQUE_SCHOOL_DATA AS (
    SELECT SOURCE,
           YEAR,
           CODE,
           NAME,
           VETFLAG,
           SCHADDR1,
           SCHADDR2,
           SCHADDR3,
           SCHADDR4,
           SCHPCODE,
           SCHEMAIL,
           CNTRY,
           OFFSHORE,
           STATE,
           DISADV,
           SCHLANG,
           SCHABSRA,
           RURAL,
           ICSEA
    FROM ODS.UAC.VW_ALL_SCHOOL
)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_SECONDARY_SCHOOL_SUM_SK,
       B.HUB_UAC_SECONDARY_SCHOOL_KEY,
       B.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(B.SOURCE, '') || ',' ||
                   IFNULL(B.YEAR, '') || ',' ||
                   IFNULL(B.CODE, '') || ',' ||
                   IFNULL(B.NAME, '') || ',' ||
                   IFNULL(B.VETFLAG, '') || ',' ||
                   IFNULL(B.SCHADDR1, '') || ',' ||
                   IFNULL(B.SCHADDR2, '') || ',' ||
                   IFNULL(B.SCHADDR3, '') || ',' ||
                   IFNULL(B.SCHADDR4, '') || ',' ||
                   IFNULL(B.SCHPCODE, 0) || ',' ||
                   IFNULL(B.SCHEMAIL, '') || ',' ||
                   IFNULL(B.CNTRY, '') || ',' ||
                   IFNULL(B.OFFSHORE, '') || ',' ||
                   IFNULL(B.STATE, '') || ',' ||
                   IFNULL(B.DISADV, '') || ',' ||
                   IFNULL(B.SCHLANG, '') || ',' ||
                   IFNULL(B.SCHABSRA, '') || ',' ||
                   IFNULL(B.RURAL, '') || ',' ||
                   IFNULL(B.ICSEA, '') || ',' ||
                   IFNULL(B.LATITUDE, 0) || ',' ||
                   IFNULL(B.LONGITUDE, 0) || ',' ||
                   IFNULL(B.MESH_BLOCK, 'Unknown') || ',' ||
                   IFNULL(B.DISTANCE_FROM_CAMPUS, 0) || ',' ||
                   IFNULL('N', '')
           )                                     HASH_MD5,
       YEAR,
       CODE,
       NAME,
       VETFLAG,
       SCHADDR1,
       SCHADDR2,
       SCHADDR3,
       SCHADDR4,
       SCHPCODE,
       SCHEMAIL,
       CNTRY,
       OFFSHORE,
       STATE,
       DISADV,
       SCHLANG,
       SCHABSRA,
       RURAL,
       ICSEA,
       LATITUDE,
       LONGITUDE,
       COALESCE(MESH_BLOCK, 'Unknown'),
       DISTANCE_FROM_CAMPUS,
       'N'                                       IS_DELETED
FROM (SELECT MD5(
                         IFNULL(CODE, '') || ',' ||
                         IFNULL(YEAR, '') || ',' ||
                         IFNULL(SOURCE, '') || ',' ||
                         IFNULL(STATE, '')
                 )                                  HUB_UAC_SECONDARY_SCHOOL_KEY,
             SOURCE,
             YEAR,
             CODE,
             NAME,
             VETFLAG,
             SCHADDR1,
             SCHADDR2,
             SCHADDR3,
             SCHADDR4,
             SCHPCODE,
             SCHEMAIL,
             CNTRY,
             OFFSHORE,
             STATE,
             DISADV,
             SCHLANG,
             SCHABSRA,
             RURAL,
             ICSEA,
             UAC_SCHOOL_GEO.LATITUDE,
             UAC_SCHOOL_GEO.LONGITUDE,
             MESH_BLOCK,
             HAVERSINE(UAC_SCHOOL_GEO.LATITUDE, UAC_SCHOOL_GEO.LONGITUDE, NORTH_RYDE_CAMPUS.LATITUDE,
                       NORTH_RYDE_CAMPUS.LONGITUDE) DISTANCE_FROM_CAMPUS
      FROM UNIQUE_SCHOOL_DATA S
               LEFT JOIN ODS.EXT_REF.UAC_SCHOOL_GEO UAC_SCHOOL_GEO
                         ON UPPER(UAC_SCHOOL_GEO.SCHOOL_NAME) = UPPER(S.NAME)
                             AND UAC_SCHOOL_GEO.SCHOOL_POST_CODE = S.SCHPCODE
                             AND UAC_SCHOOL_GEO.SCHOOL_STATE = S.STATE
               LEFT OUTER JOIN DATA_VAULT.CORE.REF_MQ_CAMPUS NORTH_RYDE_CAMPUS
                               ON NORTH_RYDE_CAMPUS.CAMPUS_CODE = 'U'
      WHERE S.STATE = 'NSW') B


WHERE NOT EXISTS
    (
        SELECT NULL
        FROM SAT_SEC_SCHOOL SAT_AS
        WHERE SAT_AS.EFFECTIVE_END_DTS IS NULL
          AND SAT_AS.SOURCE = B.SOURCE
          AND SAT_AS.HUB_UAC_SECONDARY_SCHOOL_KEY = B.HUB_UAC_SECONDARY_SCHOOL_KEY
          AND SAT_AS.HASH_MD5 = MD5(
                    IFNULL(B.SOURCE, '') || ',' ||
                    IFNULL(B.YEAR, '') || ',' ||
                    IFNULL(B.CODE, '') || ',' ||
                    IFNULL(B.NAME, '') || ',' ||
                    IFNULL(B.VETFLAG, '') || ',' ||
                    IFNULL(B.SCHADDR1, '') || ',' ||
                    IFNULL(B.SCHADDR2, '') || ',' ||
                    IFNULL(B.SCHADDR3, '') || ',' ||
                    IFNULL(B.SCHADDR4, '') || ',' ||
                    IFNULL(B.SCHPCODE, 0) || ',' ||
                    IFNULL(B.SCHEMAIL, '') || ',' ||
                    IFNULL(B.CNTRY, '') || ',' ||
                    IFNULL(B.OFFSHORE, '') || ',' ||
                    IFNULL(B.STATE, '') || ',' ||
                    IFNULL(B.DISADV, '') || ',' ||
                    IFNULL(B.SCHLANG, '') || ',' ||
                    IFNULL(B.SCHABSRA, '') || ',' ||
                    IFNULL(B.RURAL, '') || ',' ||
                    IFNULL(B.ICSEA, '') || ',' ||
                    IFNULL(B.LATITUDE, 0) || ',' ||
                    IFNULL(B.LONGITUDE, 0) || ',' ||
                    IFNULL(B.MESH_BLOCK, 'Unknown') || ',' ||
                    IFNULL(B.DISTANCE_FROM_CAMPUS, 0) || ',' ||
                    IFNULL('N', '')
            )
    );
