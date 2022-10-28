INSERT INTO DATA_VAULT.CORE.LNK_COURSE_ADMISSION_TO_STATUS (LNK_COURSE_ADMISSION_TO_STATUS_KEY,
                                                            HUB_COURSE_ADMISSION_KEY,
                                                            HUB_COURSE_ADMISSION_STATUS_KEY,
                                                            STU_ID,
                                                            SPK_CD,
                                                            SPK_VER_NO,
                                                            AVAIL_YR,
                                                            LOCATION_CD,
                                                            SPRD_CD,
                                                            AVAIL_KEY_NO,
                                                            SSP_NO,
                                                            SSP_STTS_NO,
                                                            SOURCE,
                                                            LOAD_DTS,
                                                            ETL_JOB_ID)
SELECT MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
           )                                       LNK_STUDENT_COURSE_ADMISSION_KEY,
       MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           )                                       HUB_COURSE_ADMISSION_KEY,
       MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
           )                                       HUB_COURSE_ADMISSION_STATUS_KEY,
       CS_SSP.STU_ID,
       CS_SPK_DET.SPK_CD,
       CS_SPK_DET.SPK_VER_NO,
       CS_SSP.AVAIL_YR,
       CS_SSP.LOCATION_CD,
       CS_SSP.SPRD_CD,
       IFNULL(CS_SSP.AVAIL_KEY_NO, 0)              AVAIL_KEY_NO,
       CS_SSP.SSP_NO,
       CS_SSP_STTS.SSP_STTS_NO,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
             ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
    AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
    AND CS_SPK_DET.SPK_CAT_CD = 'CS'
         JOIN ODS.AMIS.S1SSP_STTS_HIST CS_SSP_STTS
              ON CS_SSP_STTS.SSP_NO = CS_SSP.SSP_NO
                  AND CS_SSP_STTS.STU_ID = CS_SSP.STU_ID
WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_COURSE_ADMISSION_TO_STATUS L
        WHERE L.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
          AND L.HUB_COURSE_ADMISSION_STATUS_KEY = MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
                                                      IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
                                                      IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
            )
    );