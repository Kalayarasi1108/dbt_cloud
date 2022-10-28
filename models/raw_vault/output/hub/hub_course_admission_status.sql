SELECT MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_STATUS_KEY,
       CS_SSP_STTS.STU_ID,
       CS_SSP_STTS.SSP_NO,
       CS_SSP_STTS.SSP_STTS_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM {{source('AMIS','S1SSP_STTS_HIST')}} CS_SSP_STTS
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = CS_SSP_STTS.SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = CS_SSP_STTS.SPK_VER_NO
                  AND CS_SPK_DET.SPK_CAT_CD = 'CS'
WHERE NOT EXISTS(
        SELECT NULL
        FROM {{source('CORE','HUB_COURSE_ADMISSION_STATUS')}} H
        WHERE H.HUB_COURSE_ADMISSION_STATUS_KEY =
              MD5(IFNULL(CS_SSP_STTS.STU_ID, '') || ',' ||
                  IFNULL(CS_SSP_STTS.SSP_NO, 0) || ',' ||
                  IFNULL(CS_SSP_STTS.SSP_STTS_NO, 0)
                  )
    )
;