INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_STATUS (SAT_COURSE_ADMISSION_STATUS_SK, HUB_COURSE_ADMISSION_STATUS_KEY, SOURCE,
                                                       LOAD_DTS,
                                                       ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_STATUS_SK,
       S.HUB_COURSE_ADMISSION_STATUS_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_COURSE_ADMISSION_STATUS_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_STATUS_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_STATUS
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STTS_HIST CS_SSP_STTS
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP_STTS.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP_STTS.SPK_VER_NO
                          AND CS_SPK_DET.SPK_CAT_CD = 'CS'
        WHERE S.HUB_COURSE_ADMISSION_STATUS_KEY = MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
                                                    IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
                                                    IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
            )
    )
;