SELECT MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
           IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(CS_SSP.SSP_NO, 0)
           )                                     HUB_COURSE_ADMISSION_KEY,
       CS_SSP.STU_ID,
       CS_SPK_DET.SPK_CD,
       CS_SPK_DET.SPK_VER_NO,
       CS_SSP.AVAIL_YR,
       CS_SSP.LOCATION_CD,
       CS_SSP.SPRD_CD,
       CS_SSP.AVAIL_KEY_NO,
       CS_SSP.SSP_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM {{SOURCE('AMIS','S1SSP_STU_SPK')}} CS_SSP
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = CS_SSP.SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
WHERE CS_SSP.SSP_NO = PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM {{SOURCE('CORE','HUB_COURSE_ADMISSION')}} H
        WHERE H.HUB_COURSE_ADMISSION_KEY = MD5(IFNULL(CS_SSP.STU_ID, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(CS_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(CS_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(CS_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(CS_SSP.SSP_NO, 0)
            )
    );
