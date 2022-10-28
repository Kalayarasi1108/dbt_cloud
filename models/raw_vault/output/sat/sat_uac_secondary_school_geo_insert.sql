-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_GEO(SAT_UAC_SECONDARY_SCHOOL_GEO_SK, HUB_UAC_SECONDARY_SCHOOL_KEY,
                                                         SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5, SCHOOL_NAME,
                                                         SCHOOL_CODE, SCHOOL_POST_CODE, SCHOOL_STATE, SCHOOL_LATITUDE,
                                                         SCHOOL_LONGITUDE, SCHOOL_MESH_BLOCK,
                                                         SCHOOL_DISTANCE_FROM_CAMPUS, IS_DELETED)

WITH VW_ALL_SCHOOL AS (
    SELECT NAME,
           SCHPCODE,
           SOURCE,
           STATE,
           CODE,
           YEAR
    FROM ODS.UAC.VW_ALL_SCHOOL
)
   , SAT_SEC_SCHOOL_GEO AS (
    SELECT HUB_UAC_SECONDARY_SCHOOL_KEY,
           SOURCE,
           HASH_MD5,
           LOAD_DTS,
           LEAD(LOAD_DTS)
                OVER (PARTITION BY HUB_UAC_SECONDARY_SCHOOL_KEY,HASH_MD5 ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_GEO
)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_SECONDARY_SCHOOL_SUM_SK,
       B.HUB_UAC_SECONDARY_SCHOOL_KEY,
       'EXT_REF'                                 SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(B.SCHOOL_NAME, '') || ',' ||
                   IFNULL(B.SCHOOL_CODE, '') || ',' ||
                   IFNULL(B.SCHOOL_POST_CODE, '') || ',' ||
                   IFNULL(B.SCHOOL_STATE, '') || ',' ||
                   IFNULL(B.SCHOOL_LATITUDE, 0) || ',' ||
                   IFNULL(B.SCHOOL_LONGITUDE, 0) || ',' ||
                   IFNULL(B.SCHOOL_MESH_BLOCK, 0) || ',' ||
                   IFNULL(B.SCHOOL_DISTANCE_FROM_CAMPUS, 0) || ',' ||
                   IFNULL('N', '')
           )                                     HASH_MD5,

       B.SCHOOL_NAME,
       B.SCHOOL_CODE,
       B.SCHOOL_POST_CODE,
       B.SCHOOL_STATE,
       B.SCHOOL_LATITUDE,
       B.SCHOOL_LONGITUDE,
       B.SCHOOL_MESH_BLOCK,
       B.SCHOOL_DISTANCE_FROM_CAMPUS,
       'N'                                       IS_DELETED
FROM (
         SELECT SCHOOL_NAME,
                SCHOOL.CODE                            SCHOOL_CODE,
                SCHOOL_POST_CODE,
                SCHOOL_STATE,
                UAC_SCHOOL_GEO.LATITUDE                SCHOOL_LATITUDE,
                UAC_SCHOOL_GEO.LONGITUDE               SCHOOL_LONGITUDE,
                MESH_BLOCK                             SCHOOL_MESH_BLOCK,
                HAVERSINE(UAC_SCHOOL_GEO.LATITUDE, UAC_SCHOOL_GEO.LONGITUDE, NORTH_RYDE_CAMPUS.LATITUDE,
                          NORTH_RYDE_CAMPUS.LONGITUDE) SCHOOL_DISTANCE_FROM_CAMPUS,
                MD5(
                            IFNULL(SCHOOL.CODE, '') || ',' ||
                            IFNULL(SCHOOL.YEAR, '') || ',' ||
                            IFNULL(SCHOOL.SOURCE, '') || ',' ||
                            IFNULL(UAC_SCHOOL_GEO.SCHOOL_STATE, '')
                    )                                  HUB_UAC_SECONDARY_SCHOOL_KEY
         FROM ODS.EXT_REF.UAC_SCHOOL_GEO UAC_SCHOOL_GEO

                  LEFT JOIN VW_ALL_SCHOOL SCHOOL
                            ON UPPER(UAC_SCHOOL_GEO.SCHOOL_NAME) = UPPER(SCHOOL.NAME)
                                AND UAC_SCHOOL_GEO.SCHOOL_POST_CODE = SCHOOL.SCHPCODE
                                AND UAC_SCHOOL_GEO.SCHOOL_STATE = SCHOOL.STATE
                  LEFT OUTER JOIN DATA_VAULT.CORE.REF_MQ_CAMPUS NORTH_RYDE_CAMPUS
                                  ON NORTH_RYDE_CAMPUS.CAMPUS_CODE = 'U'
     ) B
WHERE NOT EXISTS
    (
        SELECT NULL
        FROM SAT_SEC_SCHOOL_GEO SAT_AS
        WHERE SAT_AS.EFFECTIVE_END_DTS IS NULL
          AND SAT_AS.HUB_UAC_SECONDARY_SCHOOL_KEY = B.HUB_UAC_SECONDARY_SCHOOL_KEY
          AND SAT_AS.HASH_MD5 = MD5(
                    IFNULL(B.SCHOOL_NAME, '') || ',' ||
                    IFNULL(B.SCHOOL_CODE, '') || ',' ||
                    IFNULL(B.SCHOOL_POST_CODE, '') || ',' ||
                    IFNULL(B.SCHOOL_STATE, '') || ',' ||
                    IFNULL(B.SCHOOL_LATITUDE, 0) || ',' ||
                    IFNULL(B.SCHOOL_LONGITUDE, 0) || ',' ||
                    IFNULL(B.SCHOOL_MESH_BLOCK, 0) || ',' ||
                    IFNULL(B.SCHOOL_DISTANCE_FROM_CAMPUS, 0) || ',' ||
                    IFNULL('N', '')
            )
    );