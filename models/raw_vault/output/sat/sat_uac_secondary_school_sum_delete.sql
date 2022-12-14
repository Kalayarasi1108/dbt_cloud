-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_SUM(SAT_UAC_SECONDARY_SCHOOL_SUM_SK, HUB_UAC_SECONDARY_SCHOOL_KEY,
                                                         SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5, IS_DELETED)

WITH SAT_UAC_SCHOOL_LATEST AS (
    SELECT SAT.HUB_UAC_SECONDARY_SCHOOL_KEY,
           SAT.SOURCE,
           SAT.IS_DELETED,
           LEAD(SAT.LOAD_DTS)
                OVER (PARTITION BY SAT.HUB_UAC_SECONDARY_SCHOOL_KEY,SAT.HASH_MD5 ORDER BY SAT.LOAD_DTS) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_UAC_SECONDARY_SCHOOL_SUM SAT
)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_SECONDARY_SCHOOL_SUM_SK,
       SAT.HUB_UAC_SECONDARY_SCHOOL_KEY,
       SAT.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM SAT_UAC_SCHOOL_LATEST SAT

WHERE SAT.EFFECTIVE_END_DTS IS NULL
  AND SAT.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.UAC.VW_ALL_SCHOOL S
        WHERE SAT.HUB_UAC_SECONDARY_SCHOOL_KEY = MD5(
                    IFNULL(S.CODE, '') || ',' ||
                    IFNULL(S.YEAR, '') || ',' ||
                    IFNULL(S.SOURCE, '') || ',' ||
                    IFNULL(S.STATE, '')
            )
    );