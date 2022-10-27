insert into DATA_VAULT.CORE.LNK_COURSE_ADMISSION_TO_OFFERING(LNK_COURSE_ADMISSION_TO_OFFERING_KEY, HUB_COURSE_OFFERING_KEY, HUB_COURSE_ADMISSION_KEY, STU_ID, COURSE_SPK_CD, COURSE_SPK_VER_NO, COURSE_AVAIL_YR, COURSE_LOCATION_CD, COURSE_SPRD_CD, COURSE_AVAIL_KEY_NO, COURSE_SSP_NO, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           ) LNK_COURSE_ADMISSION_TO_OFFERING_KEY,
       MD5(
                   IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                   IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                   IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                   IFNULL(CS_SSP.LOCATION_CD, '')
           )                                      HUB_COURSE_OFFERING_KEY,
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
WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_COURSE_ADMISSION_TO_OFFERING L
        WHERE L.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
          AND L.HUB_COURSE_OFFERING_KEY = MD5(
                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                    IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                    IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                    IFNULL(CS_SSP.LOCATION_CD, '')
            )
    )
;
