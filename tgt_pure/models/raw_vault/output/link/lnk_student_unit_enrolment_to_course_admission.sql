INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_COURSE_ADMISSION (LNK_STUDENT_UNIT_ENROLMENT_TO_COURSE_ADMISSION_KEY,
                                                                            HUB_UNIT_ENROLMENT_KEY,
                                                                            HUB_COURSE_ADMISSION_KEY, STU_ID,
                                                                            UNIT_SPK_CD, UNIT_SPK_VER_NO, UNIT_AVAIL_YR,
                                                                            UNIT_LOCATION_CD, UNIT_SPRD_CD,
                                                                            UNIT_AVAIL_KEY_NO, UNIT_SSP_NO,
                                                                            UNIT_PARENT_SSP_NO, COURSE_SPK_CD,
                                                                            COURSE_SPK_VER_NO, COURSE_AVAIL_YR,
                                                                            COURSE_LOCATION_CD, COURSE_SPRD_CD,
                                                                            COURSE_AVAIL_KEY_NO, COURSE_SSP_NO, SOURCE,
                                                                            LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           )                                     LNK_STUDENT_UNIT_ENROLMENT_TO_COURSE_ADMISSION_KEY,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_KEY,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           )                                     HUB_COURSE_ADMISSION_KEY,
       CS_SSP.STU_ID,
       UN_SPK_DET.SPK_CD                         UNIT_SPK_CD,
       UN_SPK_DET.SPK_VER_NO                     UNIT_SPK_VER_NO,
       UN_SSP.AVAIL_YR                           UNIT_AVAIL_YR,
       UN_SSP.LOCATION_CD                        UNIT_LOCATION_CD,
       UN_SSP.SPRD_CD                            UNIT_SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            UNIT_AVAIL_KEY_NO,
       UN_SSP.SSP_NO                             UNIT_SSP_NO,
       UN_SSP.PARENT_SSP_NO                      UNIT_PARENT_SSP_NO,
       CS_SPK_DET.SPK_CD                         COURSE_SPK_CD,
       CS_SPK_DET.SPK_VER_NO                     COURSE_SPK_VER_NO,
       CS_SSP.AVAIL_YR                           COURSE_AVAIL_YR,
       CS_SSP.LOCATION_CD                        COURSE_LOCATION_CD,
       CS_SSP.SPRD_CD                            COURSE_SPRD_CD,
       CS_SSP.AVAIL_KEY_NO                       COURSE_AVAIL_KEY_NO,
       CS_SSP.SSP_NO                             COURSE_SSP_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                  AND CS_SPK_DET.SPK_CAT_CD = 'CS'
         JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
              ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
                  AND UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_COURSE_ADMISSION L
        WHERE L.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
          AND L.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                             IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                             IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                             IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.PARENT_SSP_NO, 0)
            )
    );
