SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
           IFNULL(STU_FEES.EP_NO, 0) || ',' ||
           IFNULL(STU_FEES.FEE_SEQ_NO, 0)
           )                                     HUB_STUDENT_UNIT_FEE_KEY,
       UN_SSP.STU_ID,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       UN_SSP.AVAIL_YR,
       UN_SSP.LOCATION_CD,
       UN_SSP.SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            AVAIL_KEY_NO,
       UN_SSP.SSP_NO,
       UN_SSP.PARENT_SSP_NO,
       STU_FEES.EP_YEAR,
       STU_FEES.EP_NO,
       STU_FEES.FEE_SEQ_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('AMIS','S1SSP_STU_SPK')}} UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1STU_FEES STU_FEES
              ON STU_FEES.SSP_NO = UN_SSP.SSP_NO
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_STUDENT_UNIT_FEE')}} H
        WHERE H.HUB_STUDENT_UNIT_FEE_KEY =
              MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                  IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                  IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                  IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                  IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                  IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                  IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                  IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                  IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                  IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                  IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                  IFNULL(STU_FEES.FEE_SEQ_NO, 0)
                  )
    )
;