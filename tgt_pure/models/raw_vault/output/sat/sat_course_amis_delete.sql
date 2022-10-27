INSERT INTO DATA_VAULT.CORE.SAT_COURSE_AMIS (SAT_COURSE_AMIS_SK, HUB_COURSE_KEY, SOURCE,
                                                      LOAD_DTS,
                                                      ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_AMIS_SK,
       S.HUB_COURSE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_COURSE_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_AMIS
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SPK_DET CS_SPK_DET
        WHERE CS_SPK_DET.SPK_CAT_CD = 'CS'
          AND S.HUB_COURSE_KEY = MD5(
                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0)
            )
    )
;