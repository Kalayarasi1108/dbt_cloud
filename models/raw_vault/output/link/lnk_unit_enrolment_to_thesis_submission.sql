INSERT INTO DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_THESIS_SUBMISSION(LNK_UNIT_ENROLMENT_TO_THESIS_SUBMISSION_KEY,
                                                                    HUB_UNIT_ENROLMENT_KEY, HUB_THESIS_SUBMISSION_KEY,
                                                                    STU_ID, SPK_CD, SPK_VER_NO, AVAIL_YR, LOCATION_CD,
                                                                    SPRD_CD, AVAIL_KEY_NO, SSP_NO, PARENT_SSP_NO,
                                                                    SSP_WRK_SUBM_NO, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
           )                                     LNK_UNIT_ENROLMENT_TO_THESIS_SUBMISSION_KEY,
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
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
           IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
           )                                     HUB_THESIS_SUBMISSION_KEY,
       UN_SSP.STU_ID,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       UN_SSP.AVAIL_YR,
       UN_SSP.LOCATION_CD,
       UN_SSP.SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            AVAIL_KEY_NO,
       UN_SSP.SSP_NO,
       UN_SSP.PARENT_SSP_NO,
       STU_SPK_WS.SSP_WRK_SUBM_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1SSP_WRKSUBM_DTL STU_SPK_WS
              ON STU_SPK_WS.SSP_NO = UN_SSP.SSP_NO
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_UNIT_ENROLMENT_TO_THESIS_SUBMISSION L
        WHERE L.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                             IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                             IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                             IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.PARENT_SSP_NO, 0)
            )
          AND L.HUB_THESIS_SUBMISSION_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_NO, 0) || ',' ||
                                                IFNULL(STU_SPK_WS.SSP_WRK_SUBM_NO, 0)
            )
    )
;