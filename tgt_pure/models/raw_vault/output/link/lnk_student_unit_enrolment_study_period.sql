INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_STUDY_PERIOD(LNK_STUDENT_UNIT_ENROLMENT_STUDY_PERIOD_KEY,
                                                                    HUB_UNIT_ENROLMENT_KEY, HUB_STUDY_PERIOD_KEY,
                                                                    STU_ID, UNIT_SPK_CD, UNIT_SPK_VER_NO, UNIT_AVAIL_YR,
                                                                    UNIT_LOCATION_CD, UNIT_SPRD_CD, UNIT_AVAIL_KEY_NO,
                                                                    UNIT_SSP_NO, UNIT_PARENT_SSP_NO, SOURCE, LOAD_DTS,
                                                                    ETL_JOB_ID)

SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0)
           )                                     LNK_STUDENT_UNIT_ENROLMENT_STUDY_PERIOD_KEY,

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

       MD5(IFNULL(CYR_LOC_DET.CALDR_YR, 0) || ',' ||
           IFNULL(CYR_LOC_DET.LOCATION_CD, '') || ',' ||
           IFNULL(CYR_LOC_DET.SPRD_CD, '')
           )                                     HUB_STUDY_PERIOD_KEY,
       UN_SSP.STU_ID                             STU_ID,
       UN_SPK_DET.SPK_CD                         UNIT_SPK_CD,
       UN_SPK_DET.SPK_VER_NO                     UNIT_SPK_VER_NO,
       UN_SSP.AVAIL_YR                           UNIT_AVAIL_YR,
       UN_SSP.LOCATION_CD                        UNIT_LOCATION_CD,
       UN_SSP.SPRD_CD                            UNIT_SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            UNIT_AVAIL_KEY_NO,
       UN_SSP.SSP_NO                             UNIT_SSP_NO,
       UN_SSP.PARENT_SSP_NO                      UNIT_PARENT_SSP_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'

         JOIN ODS.AMIS.S1CYR_LOC_DET CYR_LOC_DET
              on UN_SSP.SPRD_CD = CYR_LOC_DET.SPRD_CD
                  and un_ssp.LOCATION_CD = CYR_LOC_DET.LOCATION_CD
                  and UN_SSP.AVAIL_YR = CYR_LOC_DET.CALDR_YR

WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_STUDY_PERIOD L
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
          and L.HUB_STUDY_PERIOD_KEY = MD5(IFNULL(CYR_LOC_DET.CALDR_YR, 0) || ',' ||
                                           IFNULL(CYR_LOC_DET.LOCATION_CD, '') || ',' ||
                                           IFNULL(CYR_LOC_DET.SPRD_CD, '')
            )
    )
;
