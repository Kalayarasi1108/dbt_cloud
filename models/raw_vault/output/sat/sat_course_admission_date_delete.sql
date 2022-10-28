-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_DATE (SAT_COURSE_ADMISSION_DATE_SK, HUB_COURSE_ADMISSION_KEY, SOURCE,
                                                       LOAD_DTS, ETL_JOB_ID, HASH_MD5, MUST_COMPLETE_BY,
                                                       MINIMUM_TIME_TO_COMPL_BY, COURSE_OF_STUDY_START,
                                                       MQ_CANDIDACY_DUE_BY, START_DATE, EXPECTED_COMPLETION,
                                                       PARENT_ACTIVITY_START_DATE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_DATE_SK,
       S.HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       NULL                                      MUST_COMPLETE_BY,
       NULL                                      MINIMUM_TIME_TO_COMPL_BY,
       NULL                                      COURSE_OF_STUDY_START,
       NULL                                      MQ_CANDIDACY_DUE_BY,
       NULL                                      START_DATE,
       NULL                                      EXPECTED_COMPLETION,
       NULL                                      PARENT_ACTIVITY_START_DATE,
       'Y'                                       IS_DELETED

FROM (
         SELECT HUB_COURSE_ADMISSION_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_DATE
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                 JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                      ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                          AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
        WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
          AND S.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
    )
;
