-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_DATE (SAT_COURSE_ADMISSION_DATE_SK, HUB_COURSE_ADMISSION_KEY, SOURCE,
                                                       LOAD_DTS, ETL_JOB_ID, HASH_MD5, MUST_COMPLETE_BY,
                                                       MINIMUM_TIME_TO_COMPL_BY, COURSE_OF_STUDY_START,
                                                       MQ_CANDIDACY_DUE_BY, START_DATE, EXPECTED_COMPLETION,
                                                       PARENT_ACTIVITY_START_DATE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_DATE_SK,
       B.HUB_COURSE_ADMISSION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(B.MUST_COMPLETE_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.MINIMUM_TIME_TO_COMPL_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.COURSE_OF_STUDY_START, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.MQ_CANDIDACY_DUE_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.EXPECTED_COMPLETION, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(B.PARENT_ACTIVITY_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           'N')                                  HASH_MD5,
       B.MUST_COMPLETE_BY,
       B.MINIMUM_TIME_TO_COMPL_BY,
       B.COURSE_OF_STUDY_START,
       B.MQ_CANDIDACY_DUE_BY,
       B.START_DATE,
       B.EXPECTED_COMPLETION,
       B.PARENT_ACTIVITY_START_DATE,
       'N'                                       IS_DELETED
FROM (
         SELECT A.HUB_COURSE_ADMISSION_KEY,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = 'MCOM' THEN A.EXPECTED_DATE ELSE NULL END)  MUST_COMPLETE_BY,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = 'MINCP' THEN A.EXPECTED_DATE ELSE NULL END) MINIMUM_TIME_TO_COMPL_BY,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = 'COS' THEN A.EXPECTED_DATE ELSE NULL END)   COURSE_OF_STUDY_START,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = 'CAND' THEN A.EXPECTED_DATE ELSE NULL END)  MQ_CANDIDACY_DUE_BY,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = 'STRT' THEN A.EXPECTED_DATE ELSE NULL END)  START_DATE,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = '$EXPC' THEN A.EXPECTED_DATE ELSE NULL END) EXPECTED_COMPLETION,
                MAX(CASE WHEN A.SSP_DT_TYPE_CD = '$PSAC' THEN A.EXPECTED_DATE ELSE NULL END) PARENT_ACTIVITY_START_DATE
         FROM (
                  SELECT MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                             IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                             IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                             IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                             IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                             IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                             IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                             IFNULL(CS_SSP.SSP_NO, 0)
                             ) HUB_COURSE_ADMISSION_KEY,
                         CS_SSP.SSP_NO,
                         CS_SSP_DATE.SSP_DT_TYPE_CD,
                         CS_SSP_DATE.DATE_NO,
                         CS_SSP_DATE.EXPECTED_DATE
                  FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                           JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
                                ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                                    AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                           LEFT OUTER JOIN ODS.AMIS.S1SSP_DATE CS_SSP_DATE
                                           ON CS_SSP_DATE.SSP_NO = CS_SSP.SSP_NO AND
                                              CS_SSP_DATE.SSP_DT_TYPE_CD IS NOT NULL
                  WHERE CS_SSP.SSP_NO = PARENT_SSP_NO
              ) A
         GROUP BY A.HUB_COURSE_ADMISSION_KEY
     ) B
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_ADMISSION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_COURSE_ADMISSION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_DATE) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_ADMISSION_KEY = B.HUB_COURSE_ADMISSION_KEY
          AND S.HASH_MD5 = MD5(IFNULL(B.MUST_COMPLETE_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.MINIMUM_TIME_TO_COMPL_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.COURSE_OF_STUDY_START, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.MQ_CANDIDACY_DUE_BY, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.EXPECTED_COMPLETION, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(B.PARENT_ACTIVITY_START_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               'N')
    )
;

