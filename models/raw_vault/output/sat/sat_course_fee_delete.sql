-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_FEE (SAT_COURSE_FEE_SK, HUB_COURSE_FEE_KEY, SOURCE,
                                            LOAD_DTS, ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_FEE_SK,
       S.HUB_COURSE_FEE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_COURSE_FEE_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_FEE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_FEE
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SPK_FEE SPK_FEE
                 JOIN ODS.AMIS.S1SPK_DET SPK_DET
                      ON SPK_DET.SPK_NO = SPK_FEE.SPK_NO
                          AND SPK_DET.SPK_VER_NO = SPK_FEE.SPK_VER_NO
                          AND SPK_DET.SPK_CAT_CD = 'CS'
                 JOIN ODS.AMIS.S1FEE_DET FEE_DET
                      ON SPK_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                          AND SPK_FEE.FEE_YR = FEE_DET.FEE_YR
                 JOIN ODS.AMIS.S1FEE_ASSOC_FEE FEE_ASSOC_FEE
                      ON FEE_ASSOC_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                          AND FEE_ASSOC_FEE.FEE_YR = FEE_DET.FEE_YR
                 JOIN ODS.AMIS.S1FEE_DET FEE_DET_1
                      ON FEE_DET_1.FEE_LIAB_NO = FEE_ASSOC_FEE.ASSOC_FEE_LIAB_NO
                          AND FEE_DET_1.FEE_YR = FEE_ASSOC_FEE.ASSOC_FEE_YR
        WHERE S.HUB_COURSE_FEE_KEY = MD5(IFNULL(SPK_DET.SPK_CD, '') || ',' ||
                                         IFNULL(SPK_DET.SPK_VER_NO, 0) || ',' ||
                                         IFNULL(FEE_DET_1.FEE_LIAB_NO, 0) || ',' ||
                                         IFNULL(FEE_DET_1.FEE_YR, 0)
            )
    )
;
