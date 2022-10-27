-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL (SAT_UAC_SECONDARY_SCHOOL_SK, HUB_UAC_SECONDARY_SCHOOL_KEY,
                                                      SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5, YEAR, CODE, NAME,
                                                      VETFLAG, SCHADDR1, SCHADDR2, SCHADDR3, SCHADDR4, SCHPCODE,
                                                      SCHEMAIL, CNTRY, OFFSHORE, STATE, DISADV, SCHLANG, SCHABSRA,
                                                      RURAL, ICSEA, IS_DELETED)

WITH SAT_SEC_SCHOOL AS (
    SELECT HUB_UAC_SECONDARY_SCHOOL_KEY,
           SOURCE,
           HASH_MD5,
           LOAD_DTS,
           LEAD(LOAD_DTS)
                OVER (PARTITION BY HUB_UAC_SECONDARY_SCHOOL_KEY,HASH_MD5 ORDER BY LOAD_DTS) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL
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
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_SECONDARY_SCHOOL_SK,
       MD5(
                   IFNULL(CODE, '') || ',' ||
                   IFNULL(YEAR, '') || ',' ||
                   IFNULL(SOURCE, '') || ',' ||
                   IFNULL(STATE, '')
           )                                     HUB_UAC_SECONDARY_SCHOOL_KEY,
       SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(SOURCE, '') || ',' ||
                   IFNULL(YEAR, '') || ',' ||
                   IFNULL(CODE, '') || ',' ||
                   IFNULL(NAME, '') || ',' ||
                   IFNULL(VETFLAG, '') || ',' ||
                   IFNULL(SCHADDR1, '') || ',' ||
                   IFNULL(SCHADDR2, '') || ',' ||
                   IFNULL(SCHADDR3, '') || ',' ||
                   IFNULL(SCHADDR4, '') || ',' ||
                   IFNULL(SCHPCODE, 0) || ',' ||
                   IFNULL(SCHEMAIL, '') || ',' ||
                   IFNULL(CNTRY, '') || ',' ||
                   IFNULL(OFFSHORE, '') || ',' ||
                   IFNULL(STATE, '') || ',' ||
                   IFNULL(DISADV, '') || ',' ||
                   IFNULL(SCHLANG, '') || ',' ||
                   IFNULL(SCHABSRA, '') || ',' ||
                   IFNULL(RURAL, '') || ',' ||
                   IFNULL(ICSEA, '') || ',' ||
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
       'N'                                       IS_DELETED
FROM UNIQUE_SCHOOL_DATA S

WHERE NOT EXISTS
    (
        SELECT NULL
        FROM SAT_SEC_SCHOOL SAT_AS
        WHERE SAT_AS.EFFECTIVE_END_DTS IS NULL
          AND SAT_AS.SOURCE = S.SOURCE
          AND SAT_AS.HUB_UAC_SECONDARY_SCHOOL_KEY = MD5(
                    IFNULL(S.CODE, '') || ',' ||
                    IFNULL(S.YEAR, '') || ',' ||
                    IFNULL(S.SOURCE, '') || ',' ||
                    IFNULL(S.STATE, '')
            )
          AND SAT_AS.HASH_MD5 = MD5(
                    IFNULL(S.SOURCE, '') || ',' ||
                    IFNULL(S.YEAR, '') || ',' ||
                    IFNULL(S.CODE, '') || ',' ||
                    IFNULL(S.NAME, '') || ',' ||
                    IFNULL(S.VETFLAG, '') || ',' ||
                    IFNULL(S.SCHADDR1, '') || ',' ||
                    IFNULL(S.SCHADDR2, '') || ',' ||
                    IFNULL(S.SCHADDR3, '') || ',' ||
                    IFNULL(S.SCHADDR4, '') || ',' ||
                    IFNULL(S.SCHPCODE, 0) || ',' ||
                    IFNULL(S.SCHEMAIL, '') || ',' ||
                    IFNULL(S.CNTRY, '') || ',' ||
                    IFNULL(S.OFFSHORE, '') || ',' ||
                    IFNULL(S.STATE, '') || ',' ||
                    IFNULL(S.DISADV, '') || ',' ||
                    IFNULL(S.SCHLANG, '') || ',' ||
                    IFNULL(S.SCHABSRA, '') || ',' ||
                    IFNULL(S.RURAL, '') || ',' ||
                    IFNULL(S.ICSEA, '') || ',' ||
                    IFNULL('N', '')
            )
    );